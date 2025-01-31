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
package com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.get;

import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaPropertyImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.JSONWriter;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpUtils;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServerCommandGetDatabase extends ServerCommandGetConnect {

  private static final String[] NAMES = {"GET|database/*"};

  public static void exportClass(
      final DatabaseSessionInternal db, final JSONWriter json, final SchemaClassInternal cls)
      throws IOException {
    json.beginObject();
    json.writeAttribute(db, "name", cls.getName());
    json.writeAttribute(db,
        "superClass", cls.getSuperClass() != null ? cls.getSuperClass().getName() : "");

    json.beginCollection(db, "superClasses");
    var i = 0;
    for (var oClass : cls.getSuperClasses()) {
      json.write((i > 0 ? "," : "") + "\"" + oClass.getName() + "\"");
      i++;
    }
    json.endCollection();

    json.writeAttribute(db, "alias", cls.getShortName());
    json.writeAttribute(db, "abstract", cls.isAbstract());
    json.writeAttribute(db, "strictmode", cls.isStrictMode());
    json.writeAttribute(db, "clusters", cls.getClusterIds());
    json.writeAttribute(db, "clusterSelection", cls.getClusterSelectionStrategyName());
    if (cls instanceof SchemaClassImpl) {
      final var custom = ((SchemaClassImpl) cls).getCustomInternal();
      if (custom != null && !custom.isEmpty()) {
        json.writeAttribute(db, "custom", custom);
      }
    }

    try {
      json.writeAttribute(db, "records", db.countClass(cls.getName()));
    } catch (SecurityAccessException e) {
      json.writeAttribute(db, "records", "? (Unauthorized)");
    } catch (Exception e) {
      json.writeAttribute(db, "records", "? (Error)");
    }

    if (cls.properties(db) != null && cls.properties(db).size() > 0) {
      json.beginCollection(db, "properties");
      for (final var prop : cls.properties(db)) {
        json.beginObject();
        json.writeAttribute(db, "name", prop.getName());
        if (prop.getLinkedClass() != null) {
          json.writeAttribute(db, "linkedClass", prop.getLinkedClass().getName());
        }
        if (prop.getLinkedType() != null) {
          json.writeAttribute(db, "linkedType", prop.getLinkedType().toString());
        }
        json.writeAttribute(db, "type", prop.getType().toString());
        json.writeAttribute(db, "mandatory", prop.isMandatory());
        json.writeAttribute(db, "readonly", prop.isReadonly());
        json.writeAttribute(db, "notNull", prop.isNotNull());
        json.writeAttribute(db, "min", prop.getMin());
        json.writeAttribute(db, "max", prop.getMax());
        json.writeAttribute(db, "regexp", prop.getRegexp());
        json.writeAttribute(db,
            "collate", prop.getCollate() != null ? prop.getCollate().getName() : "default");
        json.writeAttribute(db, "defaultValue", prop.getDefaultValue());

        if (prop instanceof SchemaPropertyImpl) {
          final var custom = ((SchemaPropertyImpl) prop).getCustomInternal();
          if (custom != null && !custom.isEmpty()) {
            json.writeAttribute(db, "custom", custom);
          }
        }

        json.endObject();
      }
      json.endCollection();
    }

    final var indexes = cls.getIndexesInternal(db);
    if (!indexes.isEmpty()) {
      json.beginCollection(db, "indexes");
      for (final var index : indexes) {
        json.beginObject();
        json.writeAttribute(db, "name", index.getName());
        json.writeAttribute(db, "type", index.getType());

        final var indexDefinition = index.getDefinition();
        if (indexDefinition != null && !indexDefinition.getFields().isEmpty()) {
          json.writeAttribute(db, "fields", indexDefinition.getFields());
        }
        json.endObject();
      }
      json.endCollection();
    }

    json.endObject();
  }

  @Override
  public void configure(final YouTrackDBServer server) {
    super.configure(server);
  }

  @Override
  public boolean execute(final HttpRequest iRequest, HttpResponse iResponse) throws Exception {
    var urlParts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: database/<database>");

    iRequest.getData().commandInfo = "Database info";
    iRequest.getData().commandDetail = urlParts[1];

    exec(iRequest, iResponse, urlParts);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  protected void exec(
      final HttpRequest iRequest, final HttpResponse iResponse, final String[] urlParts)
      throws InterruptedException, IOException {
    DatabaseSessionInternal db = null;
    try {
      if (urlParts.length > 2) {
        db = server.openDatabase(urlParts[1], urlParts[2], urlParts[3]);
      } else {
        db = getProfiledDatabaseInstance(iRequest);
      }

      final var buffer = new StringWriter();
      final var json = new JSONWriter(buffer);
      json.beginObject();

      json.beginObject("server");
      json.writeAttribute(db, "version", YouTrackDBConstants.getRawVersion());
      if (YouTrackDBConstants.getBuildNumber() != null) {
        json.writeAttribute(db, "build", YouTrackDBConstants.getBuildNumber());
      }
      json.writeAttribute(db, "osName", System.getProperty("os.name"));
      json.writeAttribute(db, "osVersion", System.getProperty("os.version"));
      json.writeAttribute(db, "osArch", System.getProperty("os.arch"));
      json.writeAttribute(db, "javaVendor", System.getProperty("java.vm.vendor"));
      json.writeAttribute(db, "javaVersion", System.getProperty("java.vm.version"));

      json.beginCollection(db, "conflictStrategies");

      var strategies =
          YouTrackDBEnginesManager.instance().getRecordConflictStrategy()
              .getRegisteredImplementationNames();

      var i = 0;
      for (var strategy : strategies) {
        json.write((i > 0 ? "," : "") + "\"" + strategy + "\"");
        i++;
      }
      json.endCollection();

      json.beginCollection(db, "clusterSelectionStrategies");
      var clusterSelectionStrategies =
          db.getMetadata()
              .getImmutableSchemaSnapshot()
              .getClusterSelectionFactory()
              .getRegisteredNames();
      var j = 0;
      for (var strategy : clusterSelectionStrategies) {
        json.write((j > 0 ? "," : "") + "\"" + strategy + "\"");
        j++;
      }
      json.endCollection();

      json.endObject();

      if (db.getMetadata().getImmutableSchemaSnapshot().getClasses(db) != null) {
        json.beginCollection(db, "classes");
        List<String> classNames = new ArrayList<String>();

        for (var cls : db.getMetadata().getImmutableSchemaSnapshot().getClasses(db)) {
          classNames.add(cls.getName());
        }
        Collections.sort(classNames);

        for (var className : classNames) {
          var cls = db.getMetadata().getImmutableSchemaSnapshot()
              .getClassInternal(className);

          try {
            exportClass(db, json, cls);
          } catch (Exception e) {
            LogManager.instance().error(this, "Error on exporting class '" + cls + "'", e);
          }
        }
        json.endCollection();
      }

      if (db.getClusterNames() != null) {
        json.beginCollection(db, "clusters");
        for (var clusterName : db.getClusterNames()) {
          final var clusterId = db.getClusterIdByName(clusterName);
          if (clusterId < 0) {
            continue;
          }
          try {
            final var conflictStrategy = db.getClusterRecordConflictStrategy(clusterId);

            json.beginObject();
            json.writeAttribute(db, "id", clusterId);
            json.writeAttribute(db, "name", clusterName);
            json.writeAttribute(db, "records", db.countClusterElements(clusterId));
            json.writeAttribute(db, "conflictStrategy", conflictStrategy);
            json.writeAttribute(db, "size", "-");
            json.writeAttribute(db, "filled", "-");
            json.writeAttribute(db, "maxSize", "-");
            json.writeAttribute(db, "files", "-");
          } catch (Exception e) {
            json.writeAttribute(db, "records", "? (Unauthorized)");
          }
          json.endObject();
        }
        json.endCollection();
      }

      if (db.geCurrentUser() != null) {
        json.writeAttribute(db, "currentUser", db.geCurrentUser().getName(db));

        // exportSecurityInfo(db, json);
      }
      final var idxManager = db.getMetadata().getIndexManagerInternal();
      json.beginCollection(db, "indexes");
      for (var index : idxManager.getIndexes(db)) {
        json.beginObject();
        try {
          json.writeAttribute(db, "name", index.getName());
          json.writeAttribute(db, "configuration", index.getConfiguration(db));
          // Exclude index size because it's too costly
          // json.writeAttribute("size", index.getSize());
        } catch (Exception e) {
          LogManager.instance().error(this, "Cannot serialize index configuration", e);
        }
        json.endObject();
      }
      json.endCollection();

      json.beginObject("config");

      json.beginCollection(db, "values");
      var configuration = db.getStorageInfo().getConfiguration();
      json.writeObjects(db,
          null,
          new Object[]{"name", "dateFormat", "value", configuration.getDateFormat()},
          new Object[]{"name", "dateTimeFormat", "value", configuration.getDateTimeFormat()},
          new Object[]{"name", "localeCountry", "value", configuration.getLocaleCountry()},
          new Object[]{"name", "localeLanguage", "value", configuration.getLocaleLanguage()},
          new Object[]{"name", "charSet", "value", configuration.getCharset()},
          new Object[]{"name", "timezone", "value", configuration.getTimeZone().getID()},
          new Object[]{"name", "definitionVersion", "value", configuration.getVersion()},
          new Object[]{"name", "clusterSelection", "value", configuration.getClusterSelection()},
          new Object[]{"name", "minimumClusters", "value", configuration.getMinimumClusters()},
          new Object[]{"name", "conflictStrategy", "value", configuration.getConflictStrategy()});
      json.endCollection();

      json.beginCollection(db, "properties");
      if (configuration.getProperties() != null) {
        for (var entry : configuration.getProperties()) {
          if (entry != null) {
            json.beginObject();
            json.writeAttribute(db, "name", entry.name);
            json.writeAttribute(db, "value", entry.value);
            json.endObject();
          }
        }
      }
      json.endCollection();

      json.endObject();
      json.endObject();
      json.flush();

      iResponse.send(
          HttpUtils.STATUS_OK_CODE,
          HttpUtils.STATUS_OK_DESCRIPTION,
          HttpUtils.CONTENT_JSON,
          buffer.toString(),
          null);
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

}
