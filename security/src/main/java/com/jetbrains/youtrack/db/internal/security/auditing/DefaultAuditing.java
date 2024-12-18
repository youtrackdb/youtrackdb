/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.jetbrains.youtrack.db.internal.security.auditing;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.SystemDatabase;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.AuditingOperation;
import com.jetbrains.youtrack.db.internal.core.security.AuditingService;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class DefaultAuditing
    implements AuditingService, DatabaseLifecycleListener {

  public static final String AUDITING_LOG_CLASSNAME = "OAuditingLog";

  private boolean enabled = true;
  private Integer globalRetentionDays = -1;
  private YouTrackDBInternal context;

  private final Timer timer = new Timer();
  private AuditingHook globalHook;

  private final Map<String, AuditingHook> hooks;

  private TimerTask retainTask;

  protected static final String DEFAULT_FILE_AUDITING_DB_CONFIG = "default-auditing-config.json";
  protected static final String FILE_AUDITING_DB_CONFIG = "auditing-config.json";

  private AuditingDistribConfig distribConfig;

  private SystemDBImporter systemDbImporter;

  private SecuritySystem security;

  public static final String IMPORTER_FLAG = "AUDITING_IMPORTER";

  private class AuditingDistribConfig extends AuditingConfig {

    private boolean onNodeJoinedEnabled = false;
    private String onNodeJoinedMessage = "The node ${node} has joined";

    private boolean onNodeLeftEnabled = false;
    private String onNodeLeftMessage = "The node ${node} has left";

    public AuditingDistribConfig(final EntityImpl cfg) {
      if (cfg.containsField("onNodeJoinedEnabled")) {
        onNodeJoinedEnabled = cfg.field("onNodeJoinedEnabled");
      }

      onNodeJoinedMessage = cfg.field("onNodeJoinedMessage");

      if (cfg.containsField("onNodeLeftEnabled")) {
        onNodeLeftEnabled = cfg.field("onNodeLeftEnabled");
      }

      onNodeLeftMessage = cfg.field("onNodeLeftMessage");
    }

    @Override
    public String formatMessage(final AuditingOperation op, final String subject) {
      if (op == AuditingOperation.NODEJOINED) {
        return resolveMessage(onNodeJoinedMessage, "node", subject);
      } else if (op == AuditingOperation.NODELEFT) {
        return resolveMessage(onNodeLeftMessage, "node", subject);
      }

      return subject;
    }

    @Override
    public boolean isEnabled(AuditingOperation op) {
      if (op == AuditingOperation.NODEJOINED) {
        return onNodeJoinedEnabled;
      } else if (op == AuditingOperation.NODELEFT) {
        return onNodeLeftEnabled;
      }

      return false;
    }
  }

  public DefaultAuditing() {
    hooks = new ConcurrentHashMap<String, AuditingHook>(20);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final DatabaseSessionInternal iDatabase) {
    // Don't audit system database events.
    if (iDatabase.getName().equalsIgnoreCase(SystemDatabase.SYSTEM_DB_NAME)) {
      return;
    }

    final AuditingHook hook = defaultHook(iDatabase);
    hooks.put(iDatabase.getName(), hook);
    iDatabase.registerHook(hook);
    iDatabase.registerListener(hook);
  }

  private AuditingHook defaultHook(final DatabaseSessionInternal db) {
    final File auditingFileConfig = getConfigFile(db.getName());
    String content = null;
    if (auditingFileConfig != null && auditingFileConfig.exists()) {
      content = getContent(auditingFileConfig);

    } else {
      final InputStream resourceAsStream =
          this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE_AUDITING_DB_CONFIG);
      try {
        if (resourceAsStream == null) {
          LogManager.instance().error(this, "defaultHook() resourceAsStream is null", null);
        }

        content = getString(resourceAsStream);
        if (auditingFileConfig != null) {
          try {
            auditingFileConfig.getParentFile().mkdirs();
            auditingFileConfig.createNewFile();

            final FileOutputStream f = new FileOutputStream(auditingFileConfig);
            try {
              f.write(content.getBytes());
              f.flush();
            } finally {
              f.close();
            }
          } catch (IOException e) {
            content = "{}";
            LogManager.instance().error(this, "Cannot save auditing file configuration", e);
          }
        }
      } finally {
        try {
          if (resourceAsStream != null) {
            resourceAsStream.close();
          }
        } catch (IOException e) {
          LogManager.instance().error(this, "Cannot read auditing file configuration", e);
        }
      }
    }
    final EntityImpl cfg = new EntityImpl(db).fromJSON(content, "noMap");
    return new AuditingHook(cfg, security);
  }

  private String getContent(File auditingFileConfig) {
    FileInputStream f = null;
    String content = "";
    try {
      f = new FileInputStream(auditingFileConfig);
      final byte[] buffer = new byte[(int) auditingFileConfig.length()];
      f.read(buffer);

      content = new String(buffer);

    } catch (Exception e) {
      content = "{}";
      LogManager.instance().error(this, "Cannot get auditing file configuration", e);
    } finally {
      if (f != null) {
        try {
          f.close();
        } catch (IOException e) {
          LogManager.instance().error(this, "Cannot get auditing file configuration", e);
        }
      }
    }
    return content;
  }

  public String getString(InputStream is) {

    try {
      int ch;
      final StringBuilder sb = new StringBuilder();
      while ((ch = is.read()) != -1) {
        sb.append((char) ch);
      }
      return sb.toString();
    } catch (IOException e) {
      LogManager.instance().error(this, "Cannot get default auditing file configuration", e);
      return "{}";
    }
  }

  @Override
  public void onOpen(DatabaseSessionInternal iDatabase) {
    // Don't audit system database events.
    if (iDatabase.getName().equalsIgnoreCase(SystemDatabase.SYSTEM_DB_NAME)) {
      return;
    }

    // If the database has been opened by the auditing importer, do not hook it.
    if (iDatabase.getProperty(IMPORTER_FLAG) != null) {
      return;
    }

    AuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook == null) {
      oAuditingHook = defaultHook(iDatabase);
      hooks.put(iDatabase.getName(), oAuditingHook);
    }
    iDatabase.registerHook(oAuditingHook);
    iDatabase.registerListener(oAuditingHook);
  }

  @Override
  public void onClose(DatabaseSessionInternal iDatabase) {
    final AuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook != null) {
      iDatabase.unregisterHook(oAuditingHook);
      iDatabase.unregisterListener(oAuditingHook);
    }
  }

  @Override
  public void onDrop(DatabaseSessionInternal iDatabase) {
    onClose(iDatabase);

    final AuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook != null) {
      oAuditingHook.shutdown(false);
    }

    File f = getConfigFile(iDatabase.getName());
    if (f != null && f.exists()) {
      LogManager.instance()
          .info(this, "Removing Auditing config for db : %s", iDatabase.getName());
      f.delete();
    }
  }

  private File getConfigFile(String iDatabaseName) {
    return new File(
        security.getContext().getBasePath()
            + File.separator
            + iDatabaseName
            + File.separator
            + FILE_AUDITING_DB_CONFIG);
  }

  @Override
  public void onCreateClass(DatabaseSessionInternal iDatabase, SchemaClass iClass) {
    final AuditingHook oAuditingHook = hooks.get(iDatabase.getName());

    if (oAuditingHook != null) {
      oAuditingHook.onCreateClass(iClass);
    }
  }

  @Override
  public void onDropClass(DatabaseSessionInternal iDatabase, SchemaClass iClass) {
    final AuditingHook oAuditingHook = hooks.get(iDatabase.getName());

    if (oAuditingHook != null) {
      oAuditingHook.onDropClass(iClass);
    }
  }

  @Override
  public void onLocalNodeConfigurationRequest(EntityImpl iConfiguration) {
  }

  protected void updateConfigOnDisk(final String iDatabaseName, final EntityImpl cfg)
      throws IOException {
    final File auditingFileConfig = getConfigFile(iDatabaseName);
    if (auditingFileConfig != null) {
      final FileOutputStream f = new FileOutputStream(auditingFileConfig);
      try {
        f.write(cfg.toJSON("prettyPrint=true").getBytes());
        f.flush();
      } finally {
        f.close();
      }
    }
  }

  /// ///
  // ODistributedLifecycleListener
  public boolean onNodeJoining(String iNode) {
    return true;
  }

  public void onNodeJoined(DatabaseSessionInternal session, String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(AuditingOperation.NODEJOINED)) {
      log(session,
          AuditingOperation.NODEJOINED,
          distribConfig.formatMessage(AuditingOperation.NODEJOINED, iNode));
    }
  }

  public void onNodeLeft(DatabaseSessionInternal session, String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(AuditingOperation.NODELEFT)) {
      log(session,
          AuditingOperation.NODELEFT,
          distribConfig.formatMessage(AuditingOperation.NODELEFT, iNode));
    }
  }

  @Deprecated
  public static String getClusterName(final String dbName) {
    return dbName + "_auditing";
  }

  public static String getClassName(final String dbName) {
    return dbName + AUDITING_LOG_CLASSNAME;
  }

  /// ///
  // AuditingService
  public void changeConfig(
      DatabaseSessionInternal session, final SecurityUser user, final String iDatabaseName,
      final EntityImpl cfg)
      throws IOException {

    // This should never happen, but just in case...
    // Don't audit system database events.
    if (iDatabaseName != null && iDatabaseName.equalsIgnoreCase(SystemDatabase.SYSTEM_DB_NAME)) {
      return;
    }

    hooks.put(iDatabaseName, new AuditingHook(cfg, security));

    updateConfigOnDisk(iDatabaseName, cfg);

    log(session,
        AuditingOperation.CHANGEDCONFIG,
        user, String.format(
            "The auditing configuration for the database '%s' has been changed", iDatabaseName));
  }

  public EntityImpl getConfig(final String iDatabaseName) {
    return hooks.get(iDatabaseName).getConfiguration();
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(DatabaseSessionInternal session, final AuditingOperation operation,
      final String message) {
    log(session, operation, null, null, message);
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(DatabaseSessionInternal session, final AuditingOperation operation,
      SecurityUser user, final String message) {
    log(session, operation, null, user, message);
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(
      DatabaseSessionInternal session, final AuditingOperation operation,
      final String dbName,
      SecurityUser user,
      final String message) {
    // If dbName is null, then we submit the log message to the global auditing hook.
    // Otherwise, we submit it to the hook associated with dbName.
    if (dbName != null) {
      final AuditingHook oAuditingHook = hooks.get(dbName);

      if (oAuditingHook != null) {
        oAuditingHook.log(session, operation, dbName, user, message);
      } else { // Use the global hook.
        globalHook.log(session, operation, dbName, user, message);
      }
    } else { // Use the global hook.
      String userName = null;
      if (user != null) {
        userName = user.getName(session);
      }
      if (globalHook == null) {
        LogManager.instance()
            .error(
                this,
                "Default Auditing is disabled, cannot log: op=%s db='%s' user=%s message='%s'",
                null,
                operation,
                dbName,
                userName,
                message);
      } else {
        globalHook.log(session, operation, dbName, user, message);
      }
    }
  }

  private void createClassIfNotExists() {
    final DatabaseSessionInternal currentDB = DatabaseRecordThreadLocal.instance()
        .getIfDefined();

    DatabaseSessionInternal sysdb = null;

    try {
      sysdb = context.getSystemDatabase().openSystemDatabase();

      Schema schema = sysdb.getMetadata().getSchema();
      SchemaClass cls = schema.getClass(AUDITING_LOG_CLASSNAME);

      if (cls == null) {
        cls = sysdb.getMetadata().getSchema().createClass(AUDITING_LOG_CLASSNAME);
        cls.createProperty(currentDB, "date", PropertyType.DATETIME);
        cls.createProperty(currentDB, "user", PropertyType.STRING);
        cls.createProperty(currentDB, "operation", PropertyType.BYTE);
        cls.createProperty(currentDB, "record", PropertyType.LINK);
        cls.createProperty(currentDB, "changes", PropertyType.EMBEDDED);
        cls.createProperty(currentDB, "note", PropertyType.STRING);
        cls.createProperty(currentDB, "database", PropertyType.STRING);
      }
    } catch (Exception e) {
      LogManager.instance().error(this, "Creating auditing class exception", e);
    } finally {
      if (sysdb != null) {
        sysdb.close();
      }

      if (currentDB != null) {
        DatabaseRecordThreadLocal.instance().set(currentDB);
      } else {
        DatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  /// ///
  // AuditingService (SecurityComponent)

  // Called once the Server is running.
  public void active() {
    createClassIfNotExists();

    globalHook = new AuditingHook(security);

    retainTask =
        new TimerTask() {
          public void run() {
            retainLogs();
          }
        };

    long delay = 1000L;
    long period = 1000L * 60L * 60L * 24L;

    timer.scheduleAtFixedRate(retainTask, delay, period);

    YouTrackDBEnginesManager.instance().addDbLifecycleListener(this);

    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.start();
    }
  }

  public void retainLogs() {

    if (globalRetentionDays > 0) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      c.add(Calendar.DATE, (-1) * globalRetentionDays);
      retainLogs(c.getTime());
    }
  }

  public void retainLogs(Date date) {
    long time = date.getTime();
    context
        .getSystemDatabase()
        .executeWithDB(
            (db -> {
              db.command("delete from OAuditingLog where date < ?", time).close();
              return null;
            }));
  }

  public void config(DatabaseSessionInternal session, final EntityImpl jsonConfig,
      SecuritySystem security) {
    context = security.getContext();
    this.security = security;
    try {
      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("retentionDays")) {
        globalRetentionDays = jsonConfig.field("retentionDays");
      }

      if (jsonConfig.containsField("distributed")) {
        EntityImpl distribDoc = jsonConfig.field("distributed");
        distribConfig = new AuditingDistribConfig(distribDoc);
      }

      if (jsonConfig.containsField("systemImport")) {
        EntityImpl sysImport = jsonConfig.field("systemImport");

        systemDbImporter = new SystemDBImporter(context, sysImport);
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "config()", ex);
    }
  }

  // Called on removal of the component.
  public void dispose() {
    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.shutdown();
    }

    YouTrackDBEnginesManager.instance().removeDbLifecycleListener(this);

    if (globalHook != null) {
      globalHook.shutdown(false);
      globalHook = null;
    }

    if (retainTask != null) {
      retainTask.cancel();
    }

    timer.cancel();
  }

  // SecurityComponent
  public boolean isEnabled() {
    return enabled;
  }
}
