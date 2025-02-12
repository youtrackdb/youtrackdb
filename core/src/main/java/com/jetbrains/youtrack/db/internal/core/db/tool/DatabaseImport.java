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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jetbrains.youtrack.db.api.DatabaseSession.STATUS;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.EntityFieldWalker;
import com.jetbrains.youtrack.db.internal.core.db.record.ClassTrigger;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.db.tool.importer.ConverterData;
import com.jetbrains.youtrack.db.internal.core.db.tool.importer.LinksRewriter;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.SimpleKeyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassEmbedded;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaPropertyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Identity;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicy;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.JSONReader;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJackson;
import com.jetbrains.youtrack.db.internal.core.sql.executor.RidSet;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Import data from a file into a database.
 */
public class DatabaseImport extends DatabaseImpExpAbstract {

  public static final String EXPORT_IMPORT_CLASS_NAME = "___exportImportRIDMap";
  public static final String EXPORT_IMPORT_INDEX_NAME = EXPORT_IMPORT_CLASS_NAME + "Index";

  public static final int IMPORT_RECORD_DUMP_LAP_EVERY_MS = 5000;

  private final Map<SchemaPropertyImpl, String> linkedClasses = new HashMap<>();
  private final Map<SchemaClass, List<String>> superClasses = new HashMap<>();
  private JSONReader jsonReader;
  private boolean schemaImported = false;
  private int exporterVersion = -1;
  private RID schemaRecordId;
  private RID indexMgrRecordId;

  private boolean deleteRIDMapping = true;

  private boolean preserveClusterIDs = true;
  private boolean migrateLinks = true;
  private boolean rebuildIndexes = true;

  private final Set<String> indexesToRebuild = new HashSet<>();
  private final Map<String, String> convertedClassNames = new HashMap<>();

  private final Int2IntOpenHashMap clusterToClusterMapping = new Int2IntOpenHashMap();

  private int maxRidbagStringSizeBeforeLazyImport = 100_000_000;
  private final ObjectMapper objectMapper;

  public DatabaseImport(
      final DatabaseSessionInternal database,
      final String fileName,
      final CommandOutputListener outputListener)
      throws IOException {
    super(database, fileName, outputListener);
    if (database.isRemote()) {
      throw new DatabaseImportException(
          "Database import is not supported for remote databases");
    }
    objectMapper = new ObjectMapper();

    clusterToClusterMapping.defaultReturnValue(-2);
    // TODO: check unclosed stream?
    final var bufferedInputStream =
        new BufferedInputStream(new FileInputStream(this.fileName));
    bufferedInputStream.mark(1024);
    InputStream inputStream;
    try {
      inputStream = new GZIPInputStream(bufferedInputStream, 16384); // 16KB
    } catch (final Exception ignore) {
      bufferedInputStream.reset();
      inputStream = bufferedInputStream;
    }
    createJsonReaderDefaultListenerAndDeclareIntent(database, outputListener, inputStream);
  }

  public DatabaseImport(
      final DatabaseSessionInternal database,
      final InputStream inputStream,
      final CommandOutputListener outputListener)
      throws IOException {
    super(database, "streaming", outputListener);
    objectMapper = new ObjectMapper();
    clusterToClusterMapping.defaultReturnValue(-2);
    createJsonReaderDefaultListenerAndDeclareIntent(database, outputListener, inputStream);
  }

  private void createJsonReaderDefaultListenerAndDeclareIntent(
      final DatabaseSessionInternal database,
      final CommandOutputListener outputListener,
      final InputStream inputStream) {
    if (outputListener == null) {
      listener = text -> {
      };
    }
    jsonReader = new JSONReader(new InputStreamReader(inputStream));
  }

  @Override
  public DatabaseImport setOptions(final String options) {
    super.setOptions(options);
    return this;
  }

  @Override
  public void run() {
    importDatabase();
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-deleteRIDMapping")) {
      deleteRIDMapping = Boolean.parseBoolean(items.getFirst());
    } else {
      if (option.equalsIgnoreCase("-preserveClusterIDs")) {
        preserveClusterIDs = Boolean.parseBoolean(items.getFirst());
      } else {

        if (option.equalsIgnoreCase("-migrateLinks")) {
          migrateLinks = Boolean.parseBoolean(items.getFirst());
        } else {
          if (option.equalsIgnoreCase("-rebuildIndexes")) {
            rebuildIndexes = Boolean.parseBoolean(items.getFirst());
          } else {
            super.parseSetting(option, items);
          }
        }
      }
    }
  }

  public DatabaseImport importDatabase() {
    database.checkSecurity(Rule.ResourceGeneric.DATABASE, Role.PERMISSION_ALL);
    final var preValidation = database.isValidationEnabled();
    try {
      listener.onMessage(
          "\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");
      final var time = System.nanoTime();

      jsonReader.readNext(JSONReader.BEGIN_OBJECT);
      database.setValidationEnabled(false);
      database.setUser(null);

      removeDefaultNonSecurityClasses();
      database.getMetadata().getIndexManagerInternal().reload(database);

      for (final var index :
          database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
        if (index.isAutomatic()) {
          indexesToRebuild.add(index.getName());
        }
      }

      var beforeImportSchemaSnapshot = database.getMetadata().getImmutableSchemaSnapshot();

      var clustersImported = false;
      while (jsonReader.hasNext() && jsonReader.lastChar() != '}') {
        final var tag = jsonReader.readString(JSONReader.FIELD_ASSIGNMENT);

        if (tag.equals("info")) {
          importInfo();
        } else {
          if (tag.equals("clusters")) {
            importClusters();
            clustersImported = true;
          } else {
            if (tag.equals("schema")) {
              importSchema(clustersImported);
            } else {
              if (tag.equals("records")) {
                importRecords(beforeImportSchemaSnapshot);
              } else {
                if (tag.equals("indexes")) {
                  importIndexes();
                } else {

                  if (tag.equals("brokenRids")) {
                    processBrokenRids();
                  } else {
                    throw new DatabaseImportException(
                        "Invalid format. Found unsupported tag '" + tag + "'");
                  }
                }
              }
            }
          }
        }
      }
      if (rebuildIndexes) {
        rebuildIndexes();
      }

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();

      database.getStorage().synch();
      // status concept seems deprecated, but status `OPEN` is checked elsewhere
      database.setStatus(STATUS.OPEN);

      if (deleteRIDMapping) {
        removeExportImportRIDsMap();
      }
      listener.onMessage(
          "\n\nDatabase import completed in " + ((System.nanoTime() - time) / 1000000) + " ms");
    } catch (final Exception e) {
      final var writer = new StringWriter();
      writer.append("Error on database import happened just before line ")
          .append(String.valueOf(jsonReader.getLineNumber())).append(", column ")
          .append(String.valueOf(jsonReader.getColumnNumber())).append("\n");
      final var printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (final IOException e1) {
        throw new DatabaseExportException(
            "Error on importing database '" + database.getDatabaseName() + "' from file: "
                + fileName, e1);
      }
      throw new DatabaseExportException(
          "Error on importing database '" + database.getDatabaseName() + "' from file: " + fileName,
          e);
    } finally {
      database.setValidationEnabled(preValidation);
      close();
    }
    return this;
  }

  private void processBrokenRids() throws IOException, ParseException {
    final Set<RID> brokenRids = new HashSet<>();
    processBrokenRids(brokenRids);
    jsonReader.readNext(JSONReader.COMMA_SEPARATOR);
  }

  // just read collection so import process can continue
  private void processBrokenRids(final Set<RID> brokenRids) throws IOException, ParseException {
    if (exporterVersion >= 12) {
      listener.onMessage(
          "Reading of set of RIDs of records which were detected as broken during database"
              + " export\n");
      jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

      do {
        jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);

        final var recordId = new RecordId(jsonReader.getValue());
        brokenRids.add(recordId);

      } while (jsonReader.lastChar() != ']');
    }
    if (migrateLinks) {
      if (exporterVersion >= 12) {
        listener.onMessage(
            brokenRids.size()
                + " were detected as broken during database export, links on those records will be"
                + " removed from result database");
      }
      migrateLinksInImportedDocuments(brokenRids);
    }
  }

  public void rebuildIndexes() {
    database.getMetadata().getIndexManagerInternal().reload(database);

    var indexManager = database.getMetadata().getIndexManagerInternal();

    listener.onMessage("\nRebuild of stale indexes...");
    for (var indexName : indexesToRebuild) {

      if (indexManager.getIndex(database, indexName) == null) {
        listener.onMessage(
            "\nIndex " + indexName + " is skipped because it is absent in imported DB.");
        continue;
      }

      listener.onMessage("\nStart rebuild index " + indexName);
      database.command("rebuild index " + indexName).close();
      listener.onMessage("\nRebuild  of index " + indexName + " is completed.");
    }
    listener.onMessage("\nStale indexes were rebuilt...");
  }

  public void removeExportImportRIDsMap() {
    listener.onMessage("\nDeleting RID Mapping table...");

    Schema schema = database.getMetadata().getSchema();
    if (schema.getClass(EXPORT_IMPORT_CLASS_NAME) != null) {
      schema.dropClass(EXPORT_IMPORT_CLASS_NAME);
    }

    listener.onMessage("OK\n");
  }

  public void close() {
  }

  public boolean isMigrateLinks() {
    return migrateLinks;
  }

  public void setMigrateLinks(boolean migrateLinks) {
    this.migrateLinks = migrateLinks;
  }

  public boolean isRebuildIndexes() {
    return rebuildIndexes;
  }

  public void setRebuildIndexes(boolean rebuildIndexes) {
    this.rebuildIndexes = rebuildIndexes;
  }

  public void setDeleteRIDMapping(boolean deleteRIDMapping) {
    this.deleteRIDMapping = deleteRIDMapping;
  }

  public void setOption(final String option, String value) {
    parseSetting("-" + option, Collections.singletonList(value));
  }

  protected void removeDefaultClusters() {
    listener.onMessage(
        "\nWARN: Exported database does not support manual index separation."
            + " Manual index cluster will be dropped.");

    // In v4 new cluster for manual indexes has been implemented. To keep database consistent we
    // should shift back all clusters and recreate cluster for manual indexes in the end.
    database.dropCluster(MetadataDefault.CLUSTER_MANUAL_INDEX_NAME);

    final Schema schema = database.getMetadata().getSchema();
    if (schema.existsClass(SecurityUserImpl.CLASS_NAME)) {
      schema.dropClass(SecurityUserImpl.CLASS_NAME);
    }
    if (schema.existsClass(Role.CLASS_NAME)) {
      schema.dropClass(Role.CLASS_NAME);
    }
    if (schema.existsClass(SecurityShared.RESTRICTED_CLASSNAME)) {
      schema.dropClass(SecurityShared.RESTRICTED_CLASSNAME);
    }
    if (schema.existsClass(Function.CLASS_NAME)) {
      schema.dropClass(Function.CLASS_NAME);
    }
    if (schema.existsClass("ORIDs")) {
      schema.dropClass("ORIDs");
    }
    if (schema.existsClass(ClassTrigger.CLASSNAME)) {
      schema.dropClass(ClassTrigger.CLASSNAME);
    }

    database.getSharedContext().getSecurity().create(database);
  }

  private void importInfo() throws IOException, ParseException {
    listener.onMessage("\nImporting database info...");

    jsonReader.readNext(JSONReader.BEGIN_OBJECT);
    while (jsonReader.lastChar() != '}') {
      final var fieldName = jsonReader.readString(JSONReader.FIELD_ASSIGNMENT);
      if (fieldName.equals("exporter-version")) {
        exporterVersion = jsonReader.readInteger(JSONReader.NEXT_IN_OBJECT);
      } else {
        if (fieldName.equals("schemaRecordId")) {
          schemaRecordId = new RecordId(jsonReader.readString(JSONReader.NEXT_IN_OBJECT));
        } else {
          if (fieldName.equals("indexMgrRecordId")) {
            indexMgrRecordId = new RecordId(jsonReader.readString(JSONReader.NEXT_IN_OBJECT));
          } else {
            jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
          }
        }
      }
    }
    jsonReader.readNext(JSONReader.COMMA_SEPARATOR);

    if (schemaRecordId == null) {
      schemaRecordId =
          new RecordId(database.getStorageInfo().getConfiguration().getSchemaRecordId());
    }

    if (indexMgrRecordId == null) {
      indexMgrRecordId =
          new RecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());
    }

    listener.onMessage("OK");
  }

  private void removeDefaultNonSecurityClasses() {
    listener.onMessage(
        "\nNon merge mode (-merge=false): removing all default non security classes");

    final Schema schema = database.getMetadata().getSchema();
    final var classes = schema.getClasses();
    final var role = schema.getClass(Role.CLASS_NAME);
    final var user = schema.getClass(SecurityUserImpl.CLASS_NAME);
    final var identity = schema.getClass(Identity.CLASS_NAME);
    // final SchemaClass oSecurityPolicy = schema.getClass(SecurityPolicy.class.getSimpleName());
    final Map<String, SchemaClass> classesToDrop = new HashMap<>();
    final Set<String> indexNames = new HashSet<>();
    for (final var dbClass : classes) {
      final var className = dbClass.getName(database);
      if (!dbClass.isSuperClassOf(database, role)
          && !dbClass.isSuperClassOf(database, user)
          && !dbClass.isSuperClassOf(database,
          identity) /*&& !dbClass.isSuperClassOf(oSecurityPolicy)*/) {
        classesToDrop.put(className, dbClass);
        indexNames.addAll(((SchemaClassInternal) dbClass).getIndexes(database));
      }
    }

    final var indexManager = database.getMetadata().getIndexManagerInternal();
    for (final var indexName : indexNames) {
      indexManager.dropIndex(database, indexName);
    }

    var removedClasses = 0;
    while (!classesToDrop.isEmpty()) {
      final AbstractList<String> classesReadyToDrop = new ArrayList<>();
      for (final var className : classesToDrop.keySet()) {
        var isSuperClass = false;
        for (var dbClass : classesToDrop.values()) {
          final var parentClasses = dbClass.getSuperClasses(database);
          if (parentClasses != null) {
            for (var parentClass : parentClasses) {
              if (className.equalsIgnoreCase(parentClass.getName(database))) {
                isSuperClass = true;
                break;
              }
            }
          }
        }
        if (!isSuperClass) {
          classesReadyToDrop.add(className);
        }
      }
      for (final var className : classesReadyToDrop) {
        schema.dropClass(className);
        classesToDrop.remove(className);
        removedClasses++;
        listener.onMessage("\n- Class " + className + " was removed.");
      }
    }
    listener.onMessage("\nRemoved " + removedClasses + " classes.");
  }

  private void setLinkedClasses() {
    for (final var linkedClass : linkedClasses.entrySet()) {
      linkedClass
          .getKey()
          .setLinkedClass(database,
              database.getMetadata().getSchema().getClass(linkedClass.getValue()));
    }
  }

  private void importSchema(boolean clustersImported) throws IOException, ParseException {
    if (!clustersImported) {
      removeDefaultClusters();
    }

    listener.onMessage("\nImporting database schema...");

    jsonReader.readNext(JSONReader.BEGIN_OBJECT);
    @SuppressWarnings("unused")
    var schemaVersion =
        jsonReader
            .readNext(JSONReader.FIELD_ASSIGNMENT)
            .checkContent("\"version\"")
            .readNumber(JSONReader.ANY_NUMBER, true);
    jsonReader.readNext(JSONReader.COMMA_SEPARATOR);
    jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);
    // This can be removed after the M1 expires
    if (jsonReader.getValue().equals("\"globalProperties\"")) {
      jsonReader.readNext(JSONReader.BEGIN_COLLECTION);
      do {
        jsonReader.readNext(JSONReader.BEGIN_OBJECT);
        jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"");
        var name = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT).checkContent("\"global-id\"");
        var id = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"");
        var type = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
        // getDatabase().getMetadata().getSchema().createGlobalProperty(name, PropertyType.valueOf(type),
        // Integer.valueOf(id));
        jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');
      jsonReader.readNext(JSONReader.COMMA_SEPARATOR);
      jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);
    }

    if (jsonReader.getValue().equals("\"blob-clusters\"")) {
      var blobClusterIds = jsonReader.readString(JSONReader.END_COLLECTION, true).trim();
      blobClusterIds = blobClusterIds.substring(1, blobClusterIds.length() - 1);

      if (!blobClusterIds.isEmpty()) {
        // READ BLOB CLUSTER IDS
        for (var i :
            StringSerializerHelper.split(
                blobClusterIds, StringSerializerHelper.RECORD_SEPARATOR)) {
          var cluster = Integer.parseInt(i);
          if (!ArrayUtils.contains(database.getBlobClusterIds(), cluster)) {
            var name = database.getClusterNameById(cluster);
            database.addBlobCluster(name);
          }
        }
      }

      jsonReader.readNext(JSONReader.COMMA_SEPARATOR);
      jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);
    }

    jsonReader.checkContent("\"classes\"").readNext(JSONReader.BEGIN_COLLECTION);

    long classImported = 0;

    try {
      do {
        jsonReader.readNext(JSONReader.BEGIN_OBJECT);
        var className =
            jsonReader
                .readNext(JSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"name\"")
                .readString(JSONReader.COMMA_SEPARATOR);

        var clusterIdsStr = jsonReader
            .readNext(JSONReader.FIELD_ASSIGNMENT)
            .checkContent("\"cluster-ids\"")
            .readString(JSONReader.END_COLLECTION, true)
            .trim();

        var classClusterIds =
            StringSerializerHelper.splitIntArray(
                clusterIdsStr.substring(1, clusterIdsStr.length() - 1));

        jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
        if (className.contains(".")) {
          // MIGRATE OLD NAME WITH . TO _
          final var newClassName = className.replace('.', '_');
          convertedClassNames.put(className, newClassName);

          listener.onMessage(
              "\nWARNING: class '" + className + "' has been renamed in '" + newClassName + "'\n");

          className = newClassName;
        }

        var cls = (SchemaClassImpl) database.getMetadata().getSchema()
            .getClass(className);

        if (cls == null) {
          if (clustersImported) {
            cls =
                (SchemaClassImpl)
                    database
                        .getMetadata()
                        .getSchema()
                        .createClass(className, classClusterIds);
          } else {
            if (className.equalsIgnoreCase("ORestricted")) {
              cls = (SchemaClassImpl) database.getMetadata().getSchema()
                  .createAbstractClass(className);
            } else {
              cls = (SchemaClassImpl) database.getMetadata().getSchema().createClass(className);
            }
          }
        }

        String value;
        while (jsonReader.lastChar() == ',') {
          jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);
          value = jsonReader.getValue();

          switch (value) {
            case "\"strictMode\"" ->
                cls.setStrictMode(database, jsonReader.readBoolean(JSONReader.NEXT_IN_OBJECT));
            case "\"abstract\"" ->
                cls.setAbstract(database, jsonReader.readBoolean(JSONReader.NEXT_IN_OBJECT));
            case "\"short-name\"" -> {
              final var shortName = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
              if (!cls.getName(database).equalsIgnoreCase(shortName)) {
                cls.setShortName(database, shortName);
              }
            }
            case "\"super-class\"" -> {
              // @compatibility <2.1 SINGLE CLASS ONLY
              final var classSuper = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
              final List<String> superClassNames = new ArrayList<String>();
              superClassNames.add(classSuper);
              superClasses.put(cls, superClassNames);
            }
            case "\"super-classes\"" -> {
              // MULTIPLE CLASSES
              jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

              final List<String> superClassNames = new ArrayList<String>();
              while (jsonReader.lastChar() != ']') {
                jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);

                final var clsName = jsonReader.getValue();

                superClassNames.add(IOUtils.getStringContent(clsName));
              }
              jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);

              superClasses.put(cls, superClassNames);
            }
            case "\"properties\"" -> {
              // GET PROPERTIES
              jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

              while (jsonReader.lastChar() != ']') {
                importProperty(cls);

                if (jsonReader.lastChar() == '}') {
                  jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);
                }
              }
              jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
            }
            case "\"customFields\"" -> {
              var customFields = importCustomFields();
              for (var entry : customFields.entrySet()) {
                cls.setCustom(database, entry.getKey(), entry.getValue());
              }
            }
            case "\"cluster-selection\"" ->
              // @SINCE 1.7
                cls.setClusterSelection(database,
                    jsonReader.readString(JSONReader.NEXT_IN_OBJECT));
          }
        }

        classImported++;

        jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');

      this.rebuildCompleteClassInheritence();
      this.setLinkedClasses();

      if (exporterVersion < 11) {
        var role = database.getMetadata().getSchema().getClass(Role.CLASS_NAME);
        role.dropProperty(database, "rules");
      }

      listener.onMessage("OK (" + classImported + " classes)");
      schemaImported = true;
      jsonReader.readNext(JSONReader.END_OBJECT);
      jsonReader.readNext(JSONReader.COMMA_SEPARATOR);
    } catch (final Exception e) {
      LogManager.instance().error(this, "Error on importing schema", e);
      listener.onMessage("ERROR (" + classImported + " entries): " + e);
    }
  }

  private void rebuildCompleteClassInheritence() {
    for (final var entry : superClasses.entrySet()) {
      for (final var superClassName : entry.getValue()) {
        final var superClass = database.getMetadata().getSchema().getClass(superClassName);

        if (!entry.getKey().getSuperClasses(database).contains(superClass)) {
          entry.getKey().addSuperClass(database, superClass);
        }
      }
    }
  }

  private void importProperty(final SchemaClassInternal iClass) throws IOException, ParseException {
    jsonReader.readNext(JSONReader.NEXT_OBJ_IN_ARRAY);

    if (jsonReader.lastChar() == ']') {
      return;
    }

    final var propName =
        jsonReader
            .readNext(JSONReader.FIELD_ASSIGNMENT)
            .checkContent("\"name\"")
            .readString(JSONReader.COMMA_SEPARATOR);

    var next = jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT).getValue();

    if (next.equals("\"id\"")) {
      // @COMPATIBILITY 1.0rc4 IGNORE THE ID
      next = jsonReader.readString(JSONReader.COMMA_SEPARATOR);
      next = jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT).getValue();
    }
    next = jsonReader.checkContent("\"type\"").readString(JSONReader.NEXT_IN_OBJECT);

    final var type = PropertyType.valueOf(next);

    String attrib;
    String value = null;

    String min = null;
    String max = null;
    String linkedClass = null;
    PropertyType linkedType = null;
    var mandatory = false;
    var readonly = false;
    var notNull = false;
    String collate = null;
    String regexp = null;
    String defaultValue = null;

    Map<String, String> customFields = null;

    while (jsonReader.lastChar() == ',') {
      jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);

      attrib = jsonReader.getValue();
      if (!attrib.equals("\"customFields\"")) {
        value =
            jsonReader.readString(
                JSONReader.NEXT_IN_OBJECT, false, JSONReader.DEFAULT_JUMP, null, false);
      }

      if (attrib.equals("\"min\"")) {
        min = value;
      } else {
        if (attrib.equals("\"max\"")) {
          max = value;
        } else {
          if (attrib.equals("\"linked-class\"")) {
            linkedClass = value;
          } else {
            if (attrib.equals("\"mandatory\"")) {
              mandatory = Boolean.parseBoolean(value);
            } else {
              if (attrib.equals("\"readonly\"")) {
                readonly = Boolean.parseBoolean(value);
              } else {
                if (attrib.equals("\"not-null\"")) {
                  notNull = Boolean.parseBoolean(value);
                } else {
                  if (attrib.equals("\"linked-type\"")) {
                    linkedType = PropertyType.valueOf(value);
                  } else {
                    if (attrib.equals("\"collate\"")) {
                      collate = value;
                    } else {
                      if (attrib.equals("\"default-value\"")) {
                        defaultValue = value;
                      } else {
                        if (attrib.equals("\"customFields\"")) {
                          customFields = importCustomFields();
                        } else {
                          if (attrib.equals("\"regexp\"")) {
                            regexp = value;
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    var prop = (SchemaPropertyImpl) iClass.getProperty(database, propName);
    if (prop == null) {
      // CREATE IT
      prop = (SchemaPropertyImpl) iClass.createProperty(database, propName, type,
          (PropertyType) null,
          true);
    }
    prop.setMandatory(database, mandatory);
    prop.setReadonly(database, readonly);
    prop.setNotNull(database, notNull);

    if (min != null) {
      prop.setMin(database, min);
    }
    if (max != null) {
      prop.setMax(database, max);
    }
    if (linkedClass != null) {
      linkedClasses.put(prop, linkedClass);
    }
    if (linkedType != null) {
      prop.setLinkedType(database, linkedType);
    }
    if (collate != null) {
      prop.setCollate(database, collate);
    }
    if (regexp != null) {
      prop.setRegexp(database, regexp);
    }
    if (defaultValue != null) {
      prop.setDefaultValue(database, value);
    }
    if (customFields != null) {
      for (var entry : customFields.entrySet()) {
        prop.setCustom(database, entry.getKey(), entry.getValue());
      }
    }
  }

  private Map<String, String> importCustomFields() throws ParseException, IOException {
    Map<String, String> result = new HashMap<>();

    jsonReader.readNext(JSONReader.BEGIN_OBJECT);

    while (jsonReader.lastChar() != '}') {
      final var key = jsonReader.readString(JSONReader.FIELD_ASSIGNMENT);
      final var value = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);

      result.put(key, value);
    }

    jsonReader.readString(JSONReader.NEXT_IN_OBJECT);

    return result;
  }

  private void importClusters() throws ParseException, IOException {
    listener.onMessage("\nImporting clusters...");

    long total = 0;

    jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

    var recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }

    @SuppressWarnings("unused")
    RecordId rid = null;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(JSONReader.BEGIN_OBJECT);

      var name =
          jsonReader
              .readNext(JSONReader.FIELD_ASSIGNMENT)
              .checkContent("\"name\"")
              .readString(JSONReader.COMMA_SEPARATOR);

      if (name.isEmpty()) {
        name = null;
      }

      name = SchemaClassImpl.decodeClassName(name);

      int clusterIdFromJson;
      if (exporterVersion < 9) {
        clusterIdFromJson =
            jsonReader
                .readNext(JSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"id\"")
                .readInteger(JSONReader.COMMA_SEPARATOR);
        var type =
            jsonReader
                .readNext(JSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"type\"")
                .readString(JSONReader.NEXT_IN_OBJECT);
      } else {
        clusterIdFromJson =
            jsonReader
                .readNext(JSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"id\"")
                .readInteger(JSONReader.NEXT_IN_OBJECT);
      }

      String type;
      if (jsonReader.lastChar() == ',') {
        type =
            jsonReader
                .readNext(JSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"type\"")
                .readString(JSONReader.NEXT_IN_OBJECT);
      } else {
        type = "PHYSICAL";
      }

      if (jsonReader.lastChar() == ',') {
        rid =
            new RecordId(
                jsonReader
                    .readNext(JSONReader.FIELD_ASSIGNMENT)
                    .checkContent("\"rid\"")
                    .readString(JSONReader.NEXT_IN_OBJECT));
      } else {
        rid = null;
      }

      listener.onMessage(
          "\n- Creating cluster " + (name != null ? "'" + name + "'" : "NULL") + "...");

      var createdClusterId = name != null ? database.getClusterIdByName(name) : -1;
      if (createdClusterId == -1) {
        // CREATE IT
        if (!preserveClusterIDs) {
          createdClusterId = database.addCluster(name);
        } else {
          if (getDatabase().getClusterNameById(clusterIdFromJson) == null) {
            createdClusterId = database.addCluster(name, clusterIdFromJson, null);
            assert createdClusterId == clusterIdFromJson;
          } else {
            createdClusterId = database.addCluster(name);
            listener.onMessage(
                "\n- WARNING cluster with id " + clusterIdFromJson + " already exists");
          }
        }
      }

      if (createdClusterId != clusterIdFromJson) {
        if (!preserveClusterIDs) {
          if (database.countClusterElements(createdClusterId - 1) == 0) {
            listener.onMessage("Found previous version: migrating old clusters...");
            database.dropCluster(name);
            database.addCluster("temp_" + createdClusterId, null);
            createdClusterId = database.addCluster(name);
          } else {
            throw new ConfigurationException(
                database.getDatabaseName(), "Imported cluster '"
                + name
                + "' has id="
                + createdClusterId
                + " different from the original: "
                + clusterIdFromJson
                + ". To continue the import drop the cluster '"
                + database.getClusterNameById(createdClusterId - 1)
                + "' that has "
                + database.countClusterElements(createdClusterId - 1)
                + " records");
          }
        } else {

          final var clazz =
              database.getMetadata().getSchema().getClassByClusterId(createdClusterId);
          if (clazz instanceof SchemaClassEmbedded) {
            ((SchemaClassEmbedded) clazz).removeClusterId(database, createdClusterId, true);
          }

          database.dropCluster(createdClusterId);
          createdClusterId = database.addCluster(name, clusterIdFromJson, null);
        }
      }
      clusterToClusterMapping.put(clusterIdFromJson, createdClusterId);

      listener.onMessage("OK, assigned id=" + createdClusterId + ", was " + clusterIdFromJson);

      total++;

      jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);
    }
    jsonReader.readNext(JSONReader.COMMA_SEPARATOR);

    listener.onMessage("\nRebuilding indexes of truncated clusters ...");

    for (final var indexName : indexesToRebuild) {
      database
          .getMetadata()
          .getIndexManagerInternal()
          .getIndex(database, indexName)
          .rebuild(database,
              new ProgressListener() {
                private long last = 0;

                @Override
                public void onBegin(Object iTask, long iTotal, Object metadata) {
                  listener.onMessage(
                      "\n- Cluster content was updated: rebuilding index '" + indexName + "'...");
                }

                @Override
                public boolean onProgress(Object iTask, long iCounter, float iPercent) {
                  final var now = System.currentTimeMillis();
                  if (last == 0) {
                    last = now;
                  } else {
                    if (now - last > 1000) {
                      listener.onMessage(
                          String.format(
                              "\nIndex '%s' is rebuilding (%.2f/100)", indexName, iPercent));
                      last = now;
                    }
                  }
                  return true;
                }

                @Override
                public void onCompletition(DatabaseSessionInternal session, Object iTask,
                    boolean iSucceed) {
                  listener.onMessage(" Index " + indexName + " was successfully rebuilt.");
                }
              });
    }
    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");

    if (recreateManualIndex) {
      database.addCluster(MetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      database.getMetadata().getIndexManagerInternal().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }
    listener.onMessage("\nDone. Imported " + total + " clusters");

    database.begin();
    if (!database.exists(
        new RecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId()))) {
      var indexEntity = new EntityImpl(database);
      indexEntity.save(MetadataDefault.CLUSTER_INTERNAL_NAME);
      database.getStorage().setIndexMgrRecordId(indexEntity.getIdentity().toString());
    }
    database.commit();

  }

  /**
   * From `exporterVersion` >= `13`, `fromStream()` will be used. However, the import is still of
   * type String, and thus has to be converted to InputStream, which can only be avoided by
   * introducing a new interface method.
   */
  private RID importRecord(HashSet<RID> recordsBeforeImport,
      Schema beforeImportSchemaSnapshot)
      throws Exception {
    database.begin();
    var ok = false;
    try {
      var recordParse =
          jsonReader.readRecordString(this.maxRidbagStringSizeBeforeLazyImport);
      var value = recordParse.getKey().trim();

      if (value.isEmpty()) {
        return null;
      }

      // JUMP EMPTY RECORDS
      while (!value.isEmpty() && value.charAt(0) != '{') {
        value = value.substring(1);
      }

      RecordAbstract record = null;

      // big ridbags (ie. supernodes) sometimes send the system OOM, so they have to be discarded at
      // this stage
      // and processed later. The following collects the positions ("value" inside the string) of
      // skipped fields.
      var skippedPartsIndexes = new IntOpenHashSet();

      try {
        try {
          record =
              RecordSerializerJackson.INSTANCE.fromString(database,
                  value,
                  null,
                  null);
        } catch (final SerializationException e) {
          if (e.getCause() instanceof SchemaException) {
            // EXTRACT CLASS NAME If ANY
            final var pos = value.indexOf("\"@class\":\"");
            if (pos > -1) {
              final var end = value.indexOf('"', pos + "\"@class\":\"".length() + 1);
              final var value1 = value.substring(0, pos + "\"@class\":\"".length());
              final var clsName = value.substring(pos + "\"@class\":\"".length(), end);
              final var value2 = value.substring(end);

              final var newClassName = convertedClassNames.get(clsName);

              value = value1 + newClassName + value2;
              // OVERWRITE CLASS NAME WITH NEW NAME
              record =
                  RecordSerializerJackson.INSTANCE.fromString(database,
                      value,
                      record,
                      null);
            }
          } else {
            throw BaseException.wrapException(
                new DatabaseImportException("Error on importing record"), e,
                database.getDatabaseName());
          }
        }

        // Incorrect record format, skip this record
        if (record == null) {
          LogManager.instance().warn(this, "Broken record was detected and will be skipped");
          return null;
        }

        if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
          recordsBeforeImport.remove(record.getIdentity());
          // JUMP THE SCHEMA
          return null;
        }

        // CHECK IF THE CLUSTER IS INCLUDED

        if (record.getIdentity().getClusterId() == 0
            && record.getIdentity().getClusterPosition() == 1) {
          recordsBeforeImport.remove(record.getIdentity());
          // JUMP INTERNAL RECORDS
          return null;
        }

        if (exporterVersion >= 3) {
          var oridsId = database.getClusterIdByName("ORIDs");
          var indexId = database.getClusterIdByName(MetadataDefault.CLUSTER_INDEX_NAME);

          if (record.getIdentity().getClusterId() == indexId
              || record.getIdentity().getClusterId() == oridsId) {
            recordsBeforeImport.remove(record.getIdentity());
            // JUMP INDEX RECORDS
            return null;
          }
        }

        final var manualIndexCluster =
            database.getClusterIdByName(MetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
        final var internalCluster =
            database.getClusterIdByName(MetadataDefault.CLUSTER_INTERNAL_NAME);
        final var indexCluster = database.getClusterIdByName(MetadataDefault.CLUSTER_INDEX_NAME);

        if (exporterVersion >= 4) {
          if (record.getIdentity().getClusterId() == manualIndexCluster) {
            // JUMP INDEX RECORDS
            recordsBeforeImport.remove(record.getIdentity());
            return null;
          }
        }

        if (record.getIdentity().equals(indexMgrRecordId)) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }

        final RID rid = record.getIdentity().copy();
        final var clusterId = rid.getClusterId();

        Entity systemRecord = null;
        var cls = beforeImportSchemaSnapshot.getClassByClusterId(clusterId);
        if (cls != null) {
          assert record instanceof EntityImpl;

          if (cls.getName(database).equals(SecurityUserImpl.CLASS_NAME)) {
            try (var resultSet =
                database.query(
                    "select from " + SecurityUserImpl.CLASS_NAME + " where name = ?",
                    ((EntityImpl) record).<String>getProperty("name"))) {
              if (resultSet.hasNext()) {
                systemRecord = resultSet.next().asEntity();
              }
            }
          } else if (cls.getName(database).equals(Role.CLASS_NAME)) {
            try (var resultSet =
                database.query(
                    "select from " + Role.CLASS_NAME + " where name = ?",
                    ((EntityImpl) record).<String>getProperty("name"))) {
              if (resultSet.hasNext()) {
                systemRecord = resultSet.next().asEntity();
              }
            }
          } else if (cls.getName(database).equals(SecurityPolicy.class.getSimpleName())) {
            try (var resultSet =
                database.query(
                    "select from " + SecurityPolicy.class.getSimpleName() + " where name = ?",
                    ((EntityImpl) record).<String>getProperty("name"))) {
              if (resultSet.hasNext()) {
                systemRecord = resultSet.next().asEntity();
              }
            }
          } else if (cls.getName(database).equals("V") || cls.getName(database).equals("E")) {
            // skip it
          } else {
            throw new IllegalStateException(
                "Class " + cls.getName(database) + " is not supported.");
          }
        }

        if ((clusterId != manualIndexCluster
            && clusterId != internalCluster
            && clusterId != indexCluster)) {
          if (systemRecord != null) {
            if (!record.getClass().isAssignableFrom(systemRecord.getClass())) {
              throw new IllegalStateException(
                  "Imported record and record stored in database under id "
                      + rid
                      + " have different types. "
                      + "Stored record class is : "
                      + record.getClass()
                      + " and imported "
                      + systemRecord.getClass()
                      + " .");
            }

            RecordInternal.setVersion(record, systemRecord.getVersion());
            RecordInternal.setIdentity(record, (RecordId) systemRecord.getIdentity());
            recordsBeforeImport.remove(systemRecord.getIdentity());
          } else {
            RecordInternal.setVersion(record, 0);
            RecordInternal.setIdentity(record, new ChangeableRecordId());
          }
          record.setDirty();

          if (!rid.equals(record.getIdentity())) {
            // SAVE IT ONLY IF DIFFERENT
            var recordRid = record.getIdentity();
            var entity = database.newEntity(EXPORT_IMPORT_CLASS_NAME);

            entity.setProperty("key", rid.toString());
            entity.setProperty("value", recordRid.toString());
          }
        }

        // import skipped records (too big to be imported before)
        if (!skippedPartsIndexes.isEmpty()) {
          for (var skippedPartsIndex : skippedPartsIndexes) {
            importSkippedRidbag(record, value, skippedPartsIndex);
          }
        }

        if (!recordParse.value.isEmpty()) {
          importSkippedRidbag(record, recordParse.getValue());
        }

      } catch (Exception t) {
        if (record != null) {
          LogManager.instance()
              .error(
                  this,
                  "Error importing record "
                      + record.getIdentity()
                      + ". Source line "
                      + jsonReader.getLineNumber()
                      + ", column "
                      + jsonReader.getColumnNumber(),
                  t);
        } else {
          LogManager.instance()
              .error(
                  this,
                  "Error importing record. Source line "
                      + jsonReader.getLineNumber()
                      + ", column "
                      + jsonReader.getColumnNumber(),
                  t);
        }

        if (!(t instanceof DatabaseException)) {
          throw t;
        }
      }

      ok = true;
      return record.getIdentity();
    } finally {
      if (ok) {
        database.commit();
      } else {
        database.rollback();
      }
    }
  }

  private void importRecords(Schema beforeImportSchemaSnapshot) throws Exception {
    long total = 0;

    final Schema schema = database.getMetadata().getSchema();
    if (schema.getClass(EXPORT_IMPORT_CLASS_NAME) != null) {
      schema.dropClass(EXPORT_IMPORT_CLASS_NAME);
    }
    final var cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
    cls.createProperty(database, "key", PropertyType.STRING);
    cls.createProperty(database, "value", PropertyType.STRING);

    jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

    long totalRecords = 0;

    listener.onMessage("\n\nImporting records...");

    // the only security records are left at this moment so we need to overwrite them
    // and then remove left overs
    final var recordsBeforeImport = new HashSet<RID>();

    for (final var clusterName : database.getClusterNames()) {
      final Iterator<DBRecord> recordIterator = database.browseCluster(clusterName);
      while (recordIterator.hasNext()) {
        recordsBeforeImport.add(recordIterator.next().getIdentity());
      }
    }

    // excluding placeholder record that exist for binary compatibility
    recordsBeforeImport.remove(new RecordId(0, 0));

    RID rid;
    RID lastRid = new ChangeableRecordId();
    final var begin = System.currentTimeMillis();
    long lastLapRecords = 0;
    var last = begin;
    Set<String> involvedClusters = new HashSet<>();

    LogManager.instance().debug(this, "Detected exporter version " + exporterVersion + ".");
    while (jsonReader.lastChar() != ']') {
      // TODO: add special handling for `exporterVersion` / `DatabaseExport.EXPORTER_VERSION` >= 13
      rid = importRecord(recordsBeforeImport, beforeImportSchemaSnapshot);

      total++;
      if (rid != null) {
        ++lastLapRecords;
        ++totalRecords;

        if (rid.getClusterId() != lastRid.getClusterId() || involvedClusters.isEmpty()) {
          involvedClusters.add(database.getClusterNameById(rid.getClusterId()));
        }
        lastRid = rid;
      }

      final var now = System.currentTimeMillis();
      if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
        final List<String> sortedClusters = new ArrayList<>(involvedClusters);
        Collections.sort(sortedClusters);

        listener.onMessage(
            String.format(
                "\n"
                    + "- Imported %,d records into clusters: %s. Total JSON records imported so for"
                    + " %,d .Total records imported so far: %,d (%,.2f/sec)",
                lastLapRecords,
                total,
                sortedClusters.size(),
                totalRecords,
                (float) lastLapRecords * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

        // RESET LAP COUNTERS
        last = now;
        lastLapRecords = 0;
        involvedClusters.clear();
      }
    }

    // remove all records which were absent in new database but
    // exist in old database
    for (final var leftOverRid : recordsBeforeImport) {
      database.executeInTx(() -> database.delete(leftOverRid));
    }

    database.getMetadata().reload();

    final Set<RID> brokenRids = new HashSet<>();
    processBrokenRids(brokenRids);

    listener.onMessage(
        String.format(
            "\n\nDone. Imported %,d records in %,.2f secs\n",
            totalRecords, ((float) (System.currentTimeMillis() - begin)) / 1000));

    jsonReader.readNext(JSONReader.COMMA_SEPARATOR);

  }

  private void importSkippedRidbag(final DBRecord record, final Map<String, RidSet> bags) {
    if (bags == null) {
      return;
    }
    var entity = (Entity) record;
    bags.forEach(
        (field, ridset) -> {
          RidBag ridbag = ((EntityInternal) record).getPropertyInternal(field);
          ridset.forEach(
              rid -> {
                ridbag.add(rid);
                entity.save();
              });
        });
  }

  private static void importSkippedRidbag(DBRecord record, String value,
      Integer skippedPartsIndex) {
    var entity = (EntityInternal) record;

    var builder = new StringBuilder();

    var nextIndex =
        StringSerializerHelper.parse(
            value,
            builder,
            skippedPartsIndex,
            -1,
            RecordSerializerJackson.PARAMETER_SEPARATOR,
            true,
            true,
            false,
            -1,
            false,
            ' ',
            '\n',
            '\r',
            '\t');

    var fieldName = IOUtils.getStringContent(builder.toString());
    RidBag bag = entity.getPropertyInternal(fieldName);

    if (!(value.charAt(nextIndex) == '[')) {
      throw new DatabaseImportException("Cannot import field: " + fieldName + " (too big)");
    }

    var ridBuffer = new StringBuilder();

    for (var i = nextIndex + 1; i < value.length() + 2; i++) {
      if (value.charAt(i) == ',' || value.charAt(i) == ']') {
        var ridString = IOUtils.getStringContent(ridBuffer.toString().trim());
        if (ridString.length() > 0) {
          var rid = new RecordId(ridString);
          bag.add(rid);
          record.save();
        }
        ridBuffer = new StringBuilder();
        if (value.charAt(i) == ']') {
          break;
        }
      } else {
        ridBuffer.append(value.charAt(i));
      }
    }
  }

  private void importIndexes() throws IOException, ParseException {
    listener.onMessage("\n\nImporting indexes ...");

    var indexManager = database.getMetadata().getIndexManagerInternal();
    indexManager.reload(database);

    jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

    var numberOfCreatedIndexes = 0;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(JSONReader.NEXT_OBJ_IN_ARRAY);
      if (jsonReader.lastChar() == ']') {
        break;
      }

      String blueprintsIndexClass = null;
      String indexName = null;
      String indexType = null;
      String indexAlgorithm = null;
      Set<String> clustersToIndex = new HashSet<>();
      IndexDefinition indexDefinition = null;
      EntityImpl metadata = null;
      Map<String, String> engineProperties = null;

      while (jsonReader.lastChar() != '}') {
        final var fieldName = jsonReader.readString(JSONReader.FIELD_ASSIGNMENT);
        if (fieldName.equals("name")) {
          indexName = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
        } else {
          if (fieldName.equals("type")) {
            indexType = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
          } else {
            if (fieldName.equals("algorithm")) {
              indexAlgorithm = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
            } else {
              if (fieldName.equals("clustersToIndex")) {
                clustersToIndex = importClustersToIndex();
              } else {
                if (fieldName.equals("definition")) {
                  indexDefinition = importIndexDefinition();
                  jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
                } else {
                  if (fieldName.equals("metadata")) {
                    final var jsonMetadata = jsonReader.readString(JSONReader.END_OBJECT, true);
                    metadata = new EntityImpl(database);
                    metadata.updateFromJSON(jsonMetadata);
                    jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
                  } else {
                    if (fieldName.equals("engineProperties")) {
                      final var jsonEngineProperties =
                          jsonReader.readString(JSONReader.END_OBJECT, true);
                      var entity = new EntityImpl(database);
                      entity.updateFromJSON(jsonEngineProperties);
                      Map<String, ?> map = entity.toMap();
                      if (map != null) {
                        map.replaceAll((k, v) -> v);
                      }
                      jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
                    } else {
                      if (fieldName.equals("blueprintsIndexClass")) {
                        blueprintsIndexClass = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      jsonReader.readNext(JSONReader.NEXT_IN_ARRAY);

      numberOfCreatedIndexes =
          dropAutoCreatedIndexesAndCountCreatedIndexes(
              indexManager,
              numberOfCreatedIndexes,
              blueprintsIndexClass,
              indexName,
              indexType,
              indexAlgorithm,
              clustersToIndex,
              indexDefinition,
              metadata.toMap());
    }
    listener.onMessage("\nDone. Created " + numberOfCreatedIndexes + " indexes.");
    jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);
  }

  private int dropAutoCreatedIndexesAndCountCreatedIndexes(
      final IndexManagerAbstract indexManager,
      int numberOfCreatedIndexes,
      final String blueprintsIndexClass,
      final String indexName,
      final String indexType,
      final String indexAlgorithm,
      final Set<String> clustersToIndex,
      IndexDefinition indexDefinition,
      final Map<String, ?> metadata) {
    if (indexName == null) {
      throw new IllegalArgumentException("Index name is missing");
    }

    // drop automatically created indexes
    if (!indexName.equalsIgnoreCase(EXPORT_IMPORT_INDEX_NAME)) {
      listener.onMessage("\n- Index '" + indexName + "'...");

      indexManager.dropIndex(database, indexName);
      indexesToRebuild.remove(indexName);
      var clusterIds = new IntArrayList();

      for (final var clusterName : clustersToIndex) {
        var id = database.getClusterIdByName(clusterName);
        if (id != -1) {
          clusterIds.add(id);
        } else {
          listener.onMessage(
              String.format(
                  "found not existent cluster '%s' in index '%s' configuration, skipping",
                  clusterName, indexName));
        }
      }
      var clusterIdsToIndex = new int[clusterIds.size()];

      var i = 0;
      for (var n = 0; n < clusterIds.size(); n++) {
        var clusterId = clusterIds.getInt(n);
        clusterIdsToIndex[i] = clusterId;
        i++;
      }

      if (indexDefinition == null) {
        indexDefinition = new SimpleKeyIndexDefinition(PropertyType.STRING);
      }

      var oldValue = GlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.getValueAsBoolean();
      GlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(
          indexDefinition.isNullValuesIgnored());
      final var index =
          indexManager.createIndex(
              database,
              indexName,
              indexType,
              indexDefinition,
              clusterIdsToIndex,
              null,
              metadata,
              indexAlgorithm);
      GlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(oldValue);
      numberOfCreatedIndexes++;
      listener.onMessage("OK");
    }
    return numberOfCreatedIndexes;
  }

  private Set<String> importClustersToIndex() throws IOException, ParseException {
    final Set<String> clustersToIndex = new HashSet<>();

    jsonReader.readNext(JSONReader.BEGIN_COLLECTION);

    while (jsonReader.lastChar() != ']') {
      final var clusterToIndex = jsonReader.readString(JSONReader.NEXT_IN_ARRAY);
      clustersToIndex.add(clusterToIndex);
    }

    jsonReader.readString(JSONReader.NEXT_IN_OBJECT);
    return clustersToIndex;
  }

  private IndexDefinition importIndexDefinition() throws IOException, ParseException {
    jsonReader.readString(JSONReader.BEGIN_OBJECT);
    jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);

    final var className = jsonReader.readString(JSONReader.NEXT_IN_OBJECT);

    jsonReader.readNext(JSONReader.FIELD_ASSIGNMENT);

    final var value = jsonReader.readString(JSONReader.END_OBJECT, true);
    final IndexDefinition indexDefinition;
    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {
    };
    var indexDefinitionMap = objectMapper.readValue(value, typeRef);
    try {
      final var indexDefClass = Class.forName(className);
      indexDefinition = (IndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
      indexDefinition.fromMap(indexDefinitionMap);
    } catch (final ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                   InstantiationException | IllegalAccessException e) {
      throw new IOException("Error during deserialization of index definition", e);
    }

    jsonReader.readNext(JSONReader.NEXT_IN_OBJECT);

    return indexDefinition;
  }

  private void migrateLinksInImportedDocuments(Set<RID> brokenRids) throws IOException {
    listener.onMessage(
        "\n\n"
            + "Started migration of links (-migrateLinks=true). Links are going to be updated"
            + " according to new RIDs:");

    final var begin = System.currentTimeMillis();
    final var last = new long[]{begin};
    final var entitiesLastLap = new long[1];

    var totalEntities = new long[1];
    var clusterNames = database.getClusterNames();
    for (var clusterName : clusterNames) {
      if (MetadataDefault.CLUSTER_INDEX_NAME.equals(clusterName)
          || MetadataDefault.CLUSTER_INTERNAL_NAME.equals(clusterName)
          || MetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(clusterName)) {
        continue;
      }

      final var entities = new long[1];
      final var prefix = new String[]{""};

      listener.onMessage("\n- Cluster " + clusterName + "...");

      final var clusterId = database.getClusterIdByName(clusterName);
      final var clusterRecords = database.countClusterElements(clusterId);
      var storage = database.getStorage();

      var positions =
          storage.ceilingPhysicalPositions(database, clusterId, new PhysicalPosition(0));
      while (positions.length > 0) {
        for (var position : positions) {
          database.executeInTx(() -> {
            var record = database.load(new RecordId(clusterId, position.clusterPosition));
            if (record instanceof EntityImpl entity) {
              rewriteLinksInDocument(database, entity, brokenRids);

              entities[0]++;
              entitiesLastLap[0]++;
              totalEntities[0]++;

              final var now = System.currentTimeMillis();
              if (now - last[0] > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
                listener.onMessage(
                    String.format(
                        "\n--- Migrated %,d of %,d records (%,.2f/sec)",
                        entities[0],
                        clusterRecords,
                        (float) entitiesLastLap[0] * 1000
                            / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

                // RESET LAP COUNTERS
                last[0] = now;
                entitiesLastLap[0] = 0;
                prefix[0] = "\n---";
              }
            }
          });
        }

        positions = storage.higherPhysicalPositions(database, clusterId,
            positions[positions.length - 1]);
      }

      listener.onMessage(
          String.format(
              "%s Completed migration of %,d records in current cluster", prefix[0], entities[0]));
    }

    listener.onMessage(String.format("\nTotal links updated: %,d", totalEntities[0]));
  }

  protected static void rewriteLinksInDocument(
      DatabaseSessionInternal session, EntityImpl entity, Set<RID> brokenRids) {
    entity = doRewriteLinksInDocument(session, entity, brokenRids);

    if (!entity.isDirty()) {
      // nothing changed
      return;
    }

    session.executeInTx(entity::save);
  }

  protected static EntityImpl doRewriteLinksInDocument(
      DatabaseSessionInternal session, EntityImpl entity, Set<RID> brokenRids) {
    final var rewriter = new LinksRewriter(new ConverterData(session, brokenRids));
    final var entityFieldWalker = new EntityFieldWalker();
    return entityFieldWalker.walkDocument(session, entity, rewriter);
  }

  public int getMaxRidbagStringSizeBeforeLazyImport() {
    return maxRidbagStringSizeBeforeLazyImport;
  }

  public void setMaxRidbagStringSizeBeforeLazyImport(int maxRidbagStringSizeBeforeLazyImport) {
    this.maxRidbagStringSizeBeforeLazyImport = maxRidbagStringSizeBeforeLazyImport;
  }
}
