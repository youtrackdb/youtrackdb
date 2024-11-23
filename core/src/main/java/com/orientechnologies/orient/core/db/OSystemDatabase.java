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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.ODefaultSecuritySystem;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.UUID;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

public class OSystemDatabase {

  public static final String SYSTEM_DB_NAME = "OSystem";

  public static final String SERVER_INFO_CLASS = "ServerInfo";
  public static final String SERVER_ID_PROPERTY = "serverId";

  private final OxygenDBInternal context;
  private String serverId;

  public OSystemDatabase(final OxygenDBInternal context) {
    this.context = context;
  }

  public String getSystemDatabaseName() {
    return OSystemDatabase.SYSTEM_DB_NAME;
  }

  /**
   * Adds the specified cluster to the class, if it doesn't already exist.
   */
  public void createCluster(final String className, final String clusterName) {
    final ODatabaseSessionInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      final ODatabaseSessionInternal sysdb = openSystemDatabase();
      try {

        if (!sysdb.existsCluster(clusterName)) {
          OSchema schema = sysdb.getMetadata().getSchema();
          OClass cls = schema.getClass(className);

          if (cls != null) {
            cls.addCluster(sysdb, clusterName);
          } else {
            OLogManager.instance()
                .error(this, "createCluster() Class name %s does not exist", null, className);
          }
        }

      } finally {
        sysdb.close();
      }

    } finally {
      if (currentDB != null) {
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  /**
   * Opens the System Database and returns an ODatabaseSessionInternal object. The caller is
   * responsible for retrieving any ThreadLocal-stored database before openSystemDatabase() is
   * called and restoring it after the database is closed.
   */
  public ODatabaseSessionInternal openSystemDatabase() {
    if (!exists()) {
      init();
    }
    return context.openNoAuthorization(SYSTEM_DB_NAME);
  }

  public <R> R execute(
      @Nonnull final BiFunction<OResultSet, ODatabaseSession, R> callback, final String sql,
      final Object... args) {
    final ODatabaseSessionInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      // BYPASS SECURITY
      try (final ODatabaseSession db = openSystemDatabase()) {
        try (OResultSet result = db.command(sql, args)) {
          return callback.apply(result, db);
        }
      }
    } finally {
      if (currentDB != null) {
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public ODocument save(final ODocument document) {
    return save(document, null);
  }

  public ODocument save(final ODocument document, final String clusterName) {
    final ODatabaseSessionInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      // BYPASS SECURITY
      final ODatabaseSessionInternal db = openSystemDatabase();
      try {
        if (clusterName != null) {
          return db.save(document, clusterName);
        } else {
          return db.save(document);
        }
      } finally {
        db.close();
      }

    } finally {
      if (currentDB != null) {
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public void init() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    final ODatabaseSessionInternal oldDbInThread = tl != null ? tl.getIfDefined() : null;
    try {
      if (!exists()) {
        OLogManager.instance()
            .info(this, "Creating the system database '%s' for current server", SYSTEM_DB_NAME);

        OxygenDBConfig config =
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build();
        ODatabaseType type = ODatabaseType.PLOCAL;
        if (context.isMemoryOnly()) {
          type = ODatabaseType.MEMORY;
        }
        context.create(SYSTEM_DB_NAME, null, null, type, config);
        try (var session = context.openNoAuthorization(SYSTEM_DB_NAME)) {
          ODefaultSecuritySystem.createSystemRoles(session);
        }
      }
      checkServerId();

    } finally {
      if (oldDbInThread != null) {
        ODatabaseRecordThreadLocal.instance().set(oldDbInThread);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  private synchronized void checkServerId() {
    try (ODatabaseSessionInternal db = openSystemDatabase()) {
      OClass clazz = db.getClass(SERVER_INFO_CLASS);
      if (clazz == null) {
        clazz = db.createClass(SERVER_INFO_CLASS);
      }
      var clz = clazz;
      db.executeInTx(
          () -> {
            OElement info;
            if (clz.count(db) == 0) {
              info = db.newElement(SERVER_INFO_CLASS);
            } else {
              info = db.browseClass(clz.getName()).next();
            }
            this.serverId = info.getProperty(SERVER_ID_PROPERTY);
            if (this.serverId == null) {
              this.serverId = UUID.randomUUID().toString();
              info.setProperty(SERVER_ID_PROPERTY, serverId);
              info.save();
            }
          });
    }
  }

  public void executeInDBScope(OCallable<Void, ODatabaseSessionInternal> callback) {
    executeWithDB(callback);
  }

  public <T> T executeWithDB(OCallable<T, ODatabaseSessionInternal> callback) {
    final ODatabaseSessionInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      try (final var db = openSystemDatabase()) {
        return callback.call(db);
      }
    } finally {
      if (currentDB != null) {
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public boolean exists() {
    return context.exists(SYSTEM_DB_NAME, null, null);
  }

  public String getServerId() {
    return serverId;
  }
}
