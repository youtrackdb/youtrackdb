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
package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.DefaultSecuritySystem;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.UUID;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

public class SystemDatabase {

  public static final String SYSTEM_DB_NAME = "OSystem";

  public static final String SERVER_INFO_CLASS = "ServerInfo";
  public static final String SERVER_ID_PROPERTY = "serverId";

  private final YouTrackDBInternal context;
  private String serverId;

  public SystemDatabase(final YouTrackDBInternal context) {
    this.context = context;
  }

  public String getSystemDatabaseName() {
    return SystemDatabase.SYSTEM_DB_NAME;
  }

  /**
   * Adds the specified cluster to the class, if it doesn't already exist.
   */
  public void createCluster(final String className, final String clusterName) {
    final var currentDB = DatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {
      final var sysdb = openSystemDatabase();
      try {

        if (!sysdb.existsCluster(clusterName)) {
          Schema schema = sysdb.getMetadata().getSchema();
          var cls = schema.getClass(className);

          if (cls != null) {
            cls.addCluster(sysdb, clusterName);
          } else {
            LogManager.instance()
                .error(this, "createCluster() Class name %s does not exist", null, className);
          }
        }

      } finally {
        sysdb.close();
      }

    } finally {
      if (currentDB != null) {
        DatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  /**
   * Opens the System Database and returns an DatabaseSessionInternal object. The caller is
   * responsible for retrieving any ThreadLocal-stored database before openSystemDatabase() is
   * called and restoring it after the database is closed.
   */
  public DatabaseSessionInternal openSystemDatabase() {
    if (!exists()) {
      init();
    }
    return context.openNoAuthorization(SYSTEM_DB_NAME);
  }

  public <R> R execute(
      @Nonnull final BiFunction<ResultSet, DatabaseSession, R> callback, final String sql,
      final Object... args) {
    final var currentDB = DatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {
      // BYPASS SECURITY
      try (final DatabaseSession db = openSystemDatabase()) {
        try (var result = db.command(sql, args)) {
          return callback.apply(result, db);
        }
      }
    } finally {
      if (currentDB != null) {
        DatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public EntityImpl save(final EntityImpl entity) {
    return save(entity, null);
  }

  public EntityImpl save(final EntityImpl entity, final String clusterName) {
    final var currentDB = DatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {
      // BYPASS SECURITY
      final var db = openSystemDatabase();
      try {
        if (clusterName != null) {
          return db.save(entity, clusterName);
        } else {
          return db.save(entity);
        }
      } finally {
        db.close();
      }

    } finally {
      if (currentDB != null) {
        DatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public void init() {
    final var tl = DatabaseRecordThreadLocal.instance();
    final var oldDbInThread = tl != null ? tl.getIfDefined() : null;
    try {
      if (!exists()) {
        LogManager.instance()
            .info(this, "Creating the system database '%s' for current server", SYSTEM_DB_NAME);

        var config =
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .addGlobalConfigurationParameter(GlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build();
        var type = DatabaseType.PLOCAL;
        if (context.isMemoryOnly()) {
          type = DatabaseType.MEMORY;
        }
        context.create(SYSTEM_DB_NAME, null, null, type, (YouTrackDBConfigImpl) config);
        try (var session = context.openNoAuthorization(SYSTEM_DB_NAME)) {
          DefaultSecuritySystem.createSystemRoles(session);
        }
      }
      checkServerId();

    } finally {
      if (oldDbInThread != null) {
        DatabaseRecordThreadLocal.instance().set(oldDbInThread);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  private synchronized void checkServerId() {
    try (var db = openSystemDatabase()) {
      var clazz = db.getClass(SERVER_INFO_CLASS);
      if (clazz == null) {
        clazz = db.createClass(SERVER_INFO_CLASS);
      }
      var clz = clazz;
      db.executeInTx(
          () -> {
            Entity info;
            if (db.query("select count(*) as count from " + clz.getName()).
                findFirst().<Long>getProperty("count") == 0) {
              info = db.newEntity(SERVER_INFO_CLASS);
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

  public void executeInDBScope(CallableFunction<Void, DatabaseSessionInternal> callback) {
    executeWithDB(callback);
  }

  public <T> T executeWithDB(CallableFunction<T, DatabaseSessionInternal> callback) {
    final var currentDB = DatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {
      try (final var db = openSystemDatabase()) {
        return callback.call(db);
      }
    } finally {
      if (currentDB != null) {
        DatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
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
