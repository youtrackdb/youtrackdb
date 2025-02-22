package com.jetbrains.youtrack.db.internal.server.tx;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.YourTracks;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.hook.DocumentHookAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.tools.config.ServerHookConfiguration;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class RemoteTransactionHookTest extends DbTestBase {
  private static final String SERVER_DIRECTORY = "./target/hook-transaction";
  private YouTrackDBServer server;

  @Before
  public void beforeTest() throws Exception {
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("youtrackdb-server-config.xml"));
    var hookConfig = new ServerHookConfiguration();
    hookConfig.clazz = CountCallHookServer.class.getName();
    server.getHookManager().addHook(hookConfig);
    server.activate();

    super.beforeTest();

    session.createClass("SomeTx");
  }

  @Override
  protected YouTrackDBImpl createContext() {
    var builder = YouTrackDBConfig.builder();
    var config = createConfig((YouTrackDBConfigBuilderImpl) builder);

    final var testConfig =
        System.getProperty("youtrackdb.test.env", DatabaseType.MEMORY.name().toLowerCase());

    if ("ci".equals(testConfig) || "release".equals(testConfig)) {
      dbType = DatabaseType.PLOCAL;
    } else {
      dbType = DatabaseType.MEMORY;
    }

    return (YouTrackDBImpl) YourTracks.remote("localhost", "root", "root", config);
  }

  @After
  public void afterTest() {
    super.afterTest();

    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }

  @Test
  @Ignore
  public void testCalledInTx() {
    var calls = new CountCallHook(session);
    session.registerHook(calls);

    session.begin();
    var doc = ((EntityImpl) session.newEntity("SomeTx"));
    doc.setProperty("name", "some");
    session.command("insert into SomeTx set name='aa' ").close();
    var res = session.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, res.next().getProperty("count"));
    res.close();
    session.command("delete from SomeTx where name='aa'").close();
    session.commit();

    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
  }

  @Test
  public void testCalledInClientTx() {
    YouTrackDB youTrackDB = new YouTrackDBImpl("embedded:", YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin)");
    var database = youTrackDB.open("test", "admin", "admin");
    var calls = new CountCallHook(database);
    database.registerHook(calls);
    database.createClassIfNotExist("SomeTx");
    database.begin();
    var doc = ((EntityImpl) database.newEntity("SomeTx"));
    doc.setProperty("name", "some");
    database.command("insert into SomeTx set name='aa' ").close();
    var res = database.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, res.next().getProperty("count"));
    res.close();
    database.command("delete from SomeTx where name='aa'").close();
    database.commit();

    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    assertEquals(1, calls.getBeforeUpdate());
    assertEquals(1, calls.getAfterUpdate());
    assertEquals(1, calls.getBeforeDelete());
    assertEquals(1, calls.getAfterDelete());
    database.close();
    youTrackDB.close();
    this.session.activateOnCurrentThread();
  }

  @Test
  @Ignore
  public void testCalledInTxServer() {
    session.begin();
    var calls = CountCallHookServer.instance;
    var doc = ((EntityImpl) session.newEntity("SomeTx"));
    doc.setProperty("name", "some");
    session.command("insert into SomeTx set name='aa' ").close();
    var res = session.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, res.next().getProperty("count"));
    res.close();
    session.command("delete from SomeTx where name='aa'").close();
    session.commit();
    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    assertEquals(1, calls.getBeforeUpdate());
    assertEquals(1, calls.getAfterUpdate());
    assertEquals(1, calls.getBeforeDelete());
    assertEquals(1, calls.getAfterDelete());
  }

  public static class CountCallHookServer extends CountCallHook {

    public CountCallHookServer(DatabaseSession database) {
      super(database);
      instance = this;
    }

    public static CountCallHookServer instance;
  }

  public static class CountCallHook extends DocumentHookAbstract {

    private int beforeCreate = 0;
    private int beforeUpdate = 0;
    private int beforeDelete = 0;
    private int afterUpdate = 0;
    private int afterCreate = 0;
    private int afterDelete = 0;

    public CountCallHook(DatabaseSession database) {
      super(database);
    }

    @Override
    public RESULT onRecordBeforeCreate(EntityImpl entity) {
      beforeCreate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterCreate(EntityImpl entity) {
      afterCreate++;
    }

    @Override
    public RESULT onRecordBeforeUpdate(EntityImpl entity) {
      beforeUpdate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterUpdate(EntityImpl entity) {
      afterUpdate++;
    }

    @Override
    public RESULT onRecordBeforeDelete(EntityImpl entity) {
      beforeDelete++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterDelete(EntityImpl entity) {
      afterDelete++;
    }

    public int getAfterCreate() {
      return afterCreate;
    }

    public int getAfterDelete() {
      return afterDelete;
    }

    public int getAfterUpdate() {
      return afterUpdate;
    }

    public int getBeforeCreate() {
      return beforeCreate;
    }

    public int getBeforeDelete() {
      return beforeDelete;
    }

    public int getBeforeUpdate() {
      return beforeUpdate;
    }
  }
}
