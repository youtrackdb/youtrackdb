package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class RemoteTransactionHookTest extends DBTestBase {

  private static final String SERVER_DIRECTORY = "./target/hook-transaction";
  private OServer server;

  @Before
  public void beforeTest() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    OServerHookConfiguration hookConfig = new OServerHookConfiguration();
    hookConfig.clazz = CountCallHookServer.class.getName();
    server.getHookManager().addHook(hookConfig);
    server.activate();

    super.beforeTest();

    db.createClass("SomeTx");
  }

  @Override
  protected YouTrackDB createContext() {
    var builder = YouTrackDBConfig.builder();
    var config = createConfig(builder);

    final String testConfig =
        System.getProperty("youtrackdb.test.env", ODatabaseType.MEMORY.name().toLowerCase());

    if ("ci".equals(testConfig) || "release".equals(testConfig)) {
      dbType = ODatabaseType.PLOCAL;
    } else {
      dbType = ODatabaseType.MEMORY;
    }

    return YouTrackDB.remote("localhost", "root", "root", config);
  }

  @After
  public void afterTest() {
    super.afterTest();

    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }

  @Test
  @Ignore
  public void testCalledInTx() {
    CountCallHook calls = new CountCallHook(db);
    db.registerHook(calls);

    db.begin();
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "some");
    db.save(doc);
    db.command("insert into SomeTx set name='aa' ").close();
    YTResultSet res = db.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, res.next().getProperty("count"));
    res.close();
    db.command("delete from SomeTx where name='aa'").close();
    db.commit();

    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    //    assertEquals(1, calls.getBeforeUpdate());
    //    assertEquals(1, calls.getAfterUpdate());
    //    assertEquals(1, calls.getBeforeDelete());
    //    assertEquals(1, calls.getAfterDelete());
  }

  @Test
  public void testCalledInClientTx() {
    YouTrackDB youTrackDB = new YouTrackDB("embedded:", YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin)");
    var database = youTrackDB.open("test", "admin", "admin");
    CountCallHook calls = new CountCallHook(database);
    database.registerHook(calls);
    database.createClassIfNotExist("SomeTx");
    database.begin();
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "some");
    database.save(doc);
    database.command("insert into SomeTx set name='aa' ").close();
    YTResultSet res = database.command("update SomeTx set name='bb' where name=\"some\"");
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
    this.db.activateOnCurrentThread();
  }

  @Test
  @Ignore
  public void testCalledInTxServer() {
    db.begin();
    CountCallHookServer calls = CountCallHookServer.instance;
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "some");
    db.save(doc);
    db.command("insert into SomeTx set name='aa' ").close();
    YTResultSet res = db.command("update SomeTx set name='bb' where name=\"some\"");
    assertEquals((Long) 1L, res.next().getProperty("count"));
    res.close();
    db.command("delete from SomeTx where name='aa'").close();
    db.commit();
    assertEquals(2, calls.getBeforeCreate());
    assertEquals(2, calls.getAfterCreate());
    assertEquals(1, calls.getBeforeUpdate());
    assertEquals(1, calls.getAfterUpdate());
    assertEquals(1, calls.getBeforeDelete());
    assertEquals(1, calls.getAfterDelete());
  }

  public static class CountCallHookServer extends CountCallHook {

    public CountCallHookServer(YTDatabaseSession database) {
      super(database);
      instance = this;
    }

    public static CountCallHookServer instance;
  }

  public static class CountCallHook extends ODocumentHookAbstract {

    private int beforeCreate = 0;
    private int beforeUpdate = 0;
    private int beforeDelete = 0;
    private int afterUpdate = 0;
    private int afterCreate = 0;
    private int afterDelete = 0;

    public CountCallHook(YTDatabaseSession database) {
      super(database);
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
    }

    @Override
    public RESULT onRecordBeforeCreate(YTDocument iDocument) {
      beforeCreate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterCreate(YTDocument iDocument) {
      afterCreate++;
    }

    @Override
    public RESULT onRecordBeforeUpdate(YTDocument iDocument) {
      beforeUpdate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterUpdate(YTDocument iDocument) {
      afterUpdate++;
    }

    @Override
    public RESULT onRecordBeforeDelete(YTDocument iDocument) {
      beforeDelete++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterDelete(YTDocument iDocument) {
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
