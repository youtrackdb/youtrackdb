/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.io.YTIOException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJackson;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Export data from a database to a file.
 */
public class DatabaseExport extends DatabaseImpExpAbstract {

  public static final int EXPORTER_VERSION = 13;

  protected JsonGenerator jsonGenerator;
  protected long recordExported;
  protected int compressionLevel = Deflater.BEST_SPEED;
  protected int compressionBuffer = 16384; // 16Kb

  private final String tempFileName;

  public DatabaseExport(
      final DatabaseSessionInternal iDatabase,
      final String iFileName,
      final CommandOutputListener iListener)
      throws IOException {
    super(iDatabase, iFileName, iListener);
    if (iDatabase.isRemote()) {
      throw new DatabaseExportException("Database export can be done only in embedded environment");
    }

    if (fileName == null) {
      throw new IllegalArgumentException("file name missing");
    }

    if (!fileName.endsWith(".gz")) {
      fileName += ".gz";
    }
    FileUtils.prepareForFileCreationOrReplacement(Paths.get(fileName), this, "exporting");

    this.tempFileName = fileName + ".tmp";
    FileUtils.prepareForFileCreationOrReplacement(Paths.get(tempFileName), this, "exporting");

    final GZIPOutputStream gzipOS =
        new GZIPOutputStream(new FileOutputStream(tempFileName), compressionBuffer) {
          {
            def.setLevel(compressionLevel);
          }
        };

    JsonFactory factory = new JsonFactory();
    jsonGenerator = factory.createGenerator(new OutputStreamWriter(gzipOS));
    jsonGenerator.writeStartObject();
  }

  public DatabaseExport(
      final DatabaseSessionInternal iDatabase,
      final OutputStream iOutputStream,
      final CommandOutputListener iListener)
      throws IOException {
    super(iDatabase, "streaming", iListener);
    this.tempFileName = null;

    var factory = new JsonFactory();
    jsonGenerator = factory.createGenerator(new OutputStreamWriter(iOutputStream));
    jsonGenerator.writeStartObject();
  }

  @Override
  public void run() {
    exportDatabase();
  }

  @Override
  public DatabaseExport setOptions(final String s) {
    super.setOptions(s);
    return this;
  }

  public DatabaseExport exportDatabase() {
    try {
      listener.onMessage(
          "\nStarted export of database '" + database.getName() + "' to " + fileName + "...");

      long time = System.nanoTime();

      exportInfo();
      exportClusters();
      exportSchema();
      exportRecords();
      exportIndexDefinitions();

      listener.onMessage(
          "\n\nDatabase export completed in " + ((System.nanoTime() - time) / 1000000) + "ms");

      jsonGenerator.flush();
    } catch (Exception e) {
      LogManager.instance()
          .error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new DatabaseExportException(
          "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    } finally {
      close();
    }
    return this;
  }

  private void exportRecords() throws IOException {
    long totalFoundRecords = 0;
    long totalExportedRecords = 0;

    int level = 1;
    listener.onMessage("\nExporting records...");

    final Set<RID> brokenRids = new HashSet<>();

    jsonGenerator.writeFieldName("records");
    jsonGenerator.writeStartArray();

    int exportedClusters = 0;
    int maxClusterId = getMaxClusterId();
    for (int i = 0; exportedClusters <= maxClusterId; ++i) {
      String clusterName = database.getClusterNameById(i);

      exportedClusters++;

      long clusterExportedRecordsTot = 0;
      if (clusterName != null) {
        // CHECK IF THE CLUSTER IS INCLUDED
        clusterExportedRecordsTot = database.countClusterElements(clusterName);
      }

      listener.onMessage(
          "\n- Cluster "
              + (clusterName != null ? "'" + clusterName + "'" : "NULL")
              + " (id="
              + i
              + ")...");

      long clusterExportedRecordsCurrent = 0;
      if (clusterName != null) {
        RecordAbstract rec = null;
        try {
          RecordIteratorCluster<Record> it = database.browseCluster(clusterName);

          while (it.hasNext()) {
            rec = (RecordAbstract) it.next();
            if (rec instanceof EntityImpl entity) {
              // CHECK IF THE CLASS OF THE DOCUMENT IS INCLUDED
              final String className =
                  entity.getClassName() != null
                      ? entity.getClassName().toUpperCase(Locale.ENGLISH)
                      : null;
            }

            if (exportRecord(
                clusterExportedRecordsTot, clusterExportedRecordsCurrent, rec, brokenRids)) {
              clusterExportedRecordsCurrent++;
            }
          }

          brokenRids.addAll(it.getBrokenRIDs());
        } catch (YTIOException e) {
          LogManager.instance()
              .error(
                  this,
                  "\nError on exporting record %s because of I/O problems",
                  e,
                  rec == null ? null : rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (Exception t) {
          if (rec != null) {
            final byte[] buffer = rec.toStream();

            LogManager.instance()
                .error(
                    this,
                    """
                        
                        Error on exporting record %s. It seems corrupted; size: %d bytes, raw\
                         content (as string):
                        ==========
                        %s
                        ==========""",
                    t,
                    rec.getIdentity(),
                    buffer.length,
                    new String(buffer));
          }
        }
      }

      listener.onMessage(
          "OK (records=" + clusterExportedRecordsCurrent + "/" + clusterExportedRecordsTot + ")");

      totalExportedRecords += clusterExportedRecordsCurrent;
      totalFoundRecords += clusterExportedRecordsTot;
    }
    jsonGenerator.writeEndArray();

    listener.onMessage(
        "\n\nDone. Exported "
            + totalExportedRecords
            + " of total "
            + totalFoundRecords
            + " records. "
            + brokenRids.size()
            + " records were detected as broken\n");

    jsonGenerator.writeFieldName("brokenRids");
    jsonGenerator.writeStartArray();

    for (final RID rid : brokenRids) {
      jsonGenerator.writeString(rid.toString());
    }
    jsonGenerator.writeEndArray();
  }

  public void close() {

    if (jsonGenerator == null) {
      return;
    }

    try {
      jsonGenerator.writeEndObject();
      jsonGenerator.close();
      jsonGenerator = null;
    } catch (IOException e) {
      LogManager.instance()
          .error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new DatabaseExportException(
          "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    }

    if (tempFileName != null) // may be null if writing to an output stream w/o file
    {
      try {
        FileUtils.atomicMoveWithFallback(Paths.get(tempFileName), Paths.get(fileName), this);
      } catch (IOException e) {
        LogManager.instance()
            .error(
                this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
        throw new DatabaseExportException(
            "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
      }
    }
  }

  private int getMaxClusterId() {
    int totalCluster = -1;
    for (String clusterName : database.getClusterNames()) {
      if (database.getClusterIdByName(clusterName) > totalCluster) {
        totalCluster = database.getClusterIdByName(clusterName);
      }
    }
    return totalCluster;
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-compressionLevel")) {
      compressionLevel = Integer.parseInt(items.getFirst());
    } else if (option.equalsIgnoreCase("-compressionBuffer")) {
      compressionBuffer = Integer.parseInt(items.getFirst());
    } else {
      super.parseSetting(option, items);
    }
  }

  private void exportClusters() throws IOException {
    listener.onMessage("\nExporting clusters...");

    jsonGenerator.writeFieldName("clusters");
    jsonGenerator.writeStartArray();
    int exportedClusters = 0;

    int maxClusterId = getMaxClusterId();

    for (int clusterId = 0; clusterId <= maxClusterId; ++clusterId) {

      final String clusterName = database.getClusterNameById(clusterId);

      // exclude removed clusters
      if (clusterName == null) {
        continue;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("name");
      jsonGenerator.writeString(clusterName);

      jsonGenerator.writeFieldName("id");
      jsonGenerator.writeNumber(clusterId);

      exportedClusters++;
      jsonGenerator.writeEndObject();
    }

    listener.onMessage("OK (" + exportedClusters + " clusters)");

    jsonGenerator.writeEndArray();
  }

  private void exportInfo() throws IOException {
    listener.onMessage("\nExporting database info...");

    jsonGenerator.writeObjectFieldStart("info");
    jsonGenerator.writeFieldName("name");
    jsonGenerator.writeString(database.getName().replace('\\', '/'));

    jsonGenerator.writeFieldName("default-cluster-id");
    jsonGenerator.writeNumber(database.getDefaultClusterId());

    jsonGenerator.writeFieldName("exporter-version");
    jsonGenerator.writeNumber(EXPORTER_VERSION);

    jsonGenerator.writeFieldName("engine-version");
    jsonGenerator.writeString(YouTrackDBConstants.getVersion());

    final String engineBuild = YouTrackDBConstants.getBuildNumber();
    if (engineBuild != null) {
      jsonGenerator.writeFieldName("engine-build");
      jsonGenerator.writeString(engineBuild);
    }

    jsonGenerator.writeNumberField("storage-config-version",
        StorageConfiguration.CURRENT_VERSION);
    jsonGenerator.writeNumberField("schema-version", SchemaShared.CURRENT_VERSION_NUMBER);
    jsonGenerator.writeStringField("schemaRecordId",
        database.getStorageInfo().getConfiguration().getSchemaRecordId());
    jsonGenerator.writeStringField("indexMgrRecordId",
        database.getStorageInfo().getConfiguration().getIndexMgrRecordId());
    jsonGenerator.writeEndObject();

    listener.onMessage("OK");
  }

  private void exportIndexDefinitions() throws IOException {
    listener.onMessage("\nExporting index info...");

    jsonGenerator.writeArrayFieldStart("indexes");

    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    indexManager.reload(database);

    final Collection<? extends Index> indexes = indexManager.getIndexes(database);

    for (Index index : indexes) {
      final String clsName =
          index.getDefinition() != null ? index.getDefinition().getClassName() : null;
      if (DatabaseImport.EXPORT_IMPORT_CLASS_NAME.equals(clsName)) {
        continue;
      }

      // CHECK TO FILTER CLASS
      listener.onMessage("\n- Index " + index.getName() + "...");
      jsonGenerator.writeStartObject();

      jsonGenerator.writeStringField("name", index.getName());
      jsonGenerator.writeStringField("type", index.getType());

      if (index.getAlgorithm() != null) {
        jsonGenerator.writeStringField("algorithm", index.getAlgorithm());
      }

      if (!index.getClusters().isEmpty()) {
        jsonGenerator.writeArrayFieldStart("clusters");
        for (String cluster : index.getClusters()) {
          jsonGenerator.writeString(cluster);
        }
        jsonGenerator.writeEndArray();
      }

      if (index.getDefinition() != null) {
        jsonGenerator.writeObjectFieldStart("definition");
        jsonGenerator.writeStringField("defClass", index.getDefinition().getClassName());

        jsonGenerator.writeFieldName("stream");
        RecordSerializerJackson.recordToJson(
            index.getDefinition().toStream(database, new EntityImpl(database)), jsonGenerator,
            null);

        jsonGenerator.writeEndObject();
      }

      final var metadata = index.getMetadata();
      if (metadata != null) {
        jsonGenerator.writeObjectFieldStart("metadata");
        RecordSerializerJackson.serializeEmbeddedMap(jsonGenerator, metadata, null);
      }

      jsonGenerator.writeEndObject();
      listener.onMessage("OK");
    }

    jsonGenerator.writeEndArray();
    listener.onMessage("\nOK (" + indexes.size() + " indexes)");
  }

  private void exportSchema() throws IOException {
    listener.onMessage("\nExporting schema...");

    jsonGenerator.writeObjectFieldStart("schema");
    final Schema schema = (database.getMetadata()).getImmutableSchemaSnapshot();
    //noinspection deprecation
    jsonGenerator.writeNumberField("version", schema.getVersion());
    jsonGenerator.writeArrayFieldStart("blob-clusters");
    for (int clusterId : database.getBlobClusterIds()) {
      jsonGenerator.writeNumber(clusterId);
    }
    jsonGenerator.writeEndArray();

    if (!schema.getClasses().isEmpty()) {
      jsonGenerator.writeArrayFieldStart("classes");

      final List<SchemaClass> classes = new ArrayList<>(schema.getClasses());
      Collections.sort(classes);

      for (SchemaClass cls : classes) {
        // CHECK TO FILTER CLASS
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("name", cls.getName());

        jsonGenerator.writeArrayFieldStart("cluster-ids");
        for (int clusterId : cls.getClusterIds()) {
          jsonGenerator.writeNumber(clusterId);
        }
        jsonGenerator.writeEndArray();

        if (cls.isStrictMode()) {
          jsonGenerator.writeBooleanField("strictMode", cls.isStrictMode());
        }
        if (!cls.getSuperClasses().isEmpty()) {
          jsonGenerator.writeArrayFieldStart("super-classes");
          for (SchemaClass superClass : cls.getSuperClasses()) {
            jsonGenerator.writeString(superClass.getName());
          }
          jsonGenerator.writeEndArray();
        }
        if (cls.getShortName() != null) {
          jsonGenerator.writeStringField("short-name", cls.getShortName());
        }
        if (cls.isAbstract()) {
          jsonGenerator.writeBooleanField("abstract", cls.isAbstract());
        }
        jsonGenerator.writeStringField("cluster-selection", cls.getClusterSelectionStrategyName());

        if (!cls.properties(database).isEmpty()) {
          jsonGenerator.writeArrayFieldStart("properties");

          final List<Property> properties = new ArrayList<>(cls.declaredProperties());
          Collections.sort(properties);

          for (Property p : properties) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("name", p.getName());
            jsonGenerator.writeStringField("type", p.getType().toString());
            if (p.isMandatory()) {
              jsonGenerator.writeBooleanField("mandatory", p.isMandatory());

            }
            if (p.isReadonly()) {
              jsonGenerator.writeBooleanField("readonly", p.isReadonly());
            }
            if (p.isNotNull()) {
              jsonGenerator.writeBooleanField("not-null", p.isNotNull());
            }
            if (p.getLinkedClass() != null) {
              jsonGenerator.writeStringField("linked-class", p.getLinkedClass().getName());
            }
            if (p.getLinkedType() != null) {
              jsonGenerator.writeStringField("linked-type", p.getLinkedType().toString());
            }
            if (p.getMin() != null) {
              jsonGenerator.writeStringField("min", p.getMin());
            }
            if (p.getMax() != null) {
              jsonGenerator.writeStringField("max", p.getMax());
            }
            if (p.getCollate() != null) {
              jsonGenerator.writeStringField("collate", p.getCollate().getName());
            }
            if (p.getDefaultValue() != null) {
              jsonGenerator.writeStringField("default-value", p.getDefaultValue());
            }
            if (p.getRegexp() != null) {
              jsonGenerator.writeStringField("regexp", p.getRegexp());
            }
            final Set<String> customKeys = p.getCustomKeys();
            final Map<String, String> custom = new HashMap<>();
            for (String key : customKeys) {
              custom.put(key, p.getCustom(key));
            }

            if (!custom.isEmpty()) {
              jsonGenerator.writeObjectFieldStart("customFields");
              for (Map.Entry<String, String> entry : custom.entrySet()) {
                jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
              }
              jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndObject();
          }
          jsonGenerator.writeEndArray();
        }
        final Set<String> customKeys = cls.getCustomKeys();
        final Map<String, String> custom = new HashMap<>();
        for (String key : customKeys) {
          custom.put(key, cls.getCustom(key));
        }

        if (!custom.isEmpty()) {
          jsonGenerator.writeObjectFieldStart("customFields");
          for (Map.Entry<String, String> entry : custom.entrySet()) {
            jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
          }
          jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();
    }

    jsonGenerator.writeEndObject();
    listener.onMessage("OK (" + schema.getClasses().size() + " classes)");
  }

  private boolean exportRecord(
      long recordTot, long recordNum, RecordAbstract rec, Set<RID> brokenRids) {
    if (rec != null) {
      try {
        final String format = RecordAbstract.BASE_FORMAT + ",dateAsLong";
        RecordSerializerJackson.recordToJson(rec, jsonGenerator, format);

        recordExported++;
        recordNum++;

        if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0) {
          listener.onMessage(".");
        }

        return true;
      } catch (final Exception t) {
        final RID rid = rec.getIdentity().copy();

        if (rid != null) {
          brokenRids.add(rid);
        }

        final byte[] buffer = rec.toStream();

        LogManager.instance()
            .error(
                this,
                """
                    
                    Error on exporting record %s. It seems corrupted; size: %d bytes, raw\
                     content (as string):
                    ==========
                    %s
                    ==========""",
                t,
                rec.getIdentity(),
                buffer.length,
                new String(buffer));
      }
    }

    return false;
  }
}
