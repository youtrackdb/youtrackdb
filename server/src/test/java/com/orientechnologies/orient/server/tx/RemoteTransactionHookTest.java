package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
  protected OxygenDB createContext() {
    var builder = OxygenDBConfig.builder();
    var config = createConfig(builder);

    final String testConfig =
        System.getProperty("oxygendb.test.env", ODatabaseType.MEMORY.name().toLowerCase());

    if ("ci".equals(testConfig) || "release".equals(testConfig)) {
      dbType = ODatabaseType.PLOCAL;
    } else {
      dbType = ODatabaseType.MEMORY;
    }

    return OxygenDB.remote("localhost", "root", "root", config);
  }

  @After
  public void afterTest() {
    super.afterTest();

    server.shutdown();

    Oxygen.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Oxygen.instance().startup();
  }

  @Test
  @Ignore
  public void testCalledInTx() {
    CountCallHook calls = new CountCallHook(db);
    db.registerHook(calls);

    db.begin();
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    db.save(doc);
    db.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = db.command("update SomeTx set name='bb' where name=\"some\"");
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
    OxygenDB oxygenDB = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig());
    oxygenDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    var database = oxygenDB.open("test", "admin", "admin");
    CountCallHook calls = new CountCallHook(database);
    database.registerHook(calls);
    database.createClassIfNotExist("SomeTx");
    database.begin();
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    database.save(doc);
    database.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = database.command("update SomeTx set name='bb' where name=\"some\"");
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
    oxygenDB.close();
    this.db.activateOnCurrentThread();
  }

  @Test
  @Ignore
  public void testCalledInTxServer() {
    db.begin();
    CountCallHookServer calls = CountCallHookServer.instance;
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "some");
    db.save(doc);
    db.command("insert into SomeTx set name='aa' ").close();
    OResultSet res = db.command("update SomeTx set name='bb' where name=\"some\"");
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

    public CountCallHookServer(ODatabaseSession database) {
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

    public CountCallHook(ODatabaseSession database) {
      super(database);
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
    }

    @Override
    public RESULT onRecordBeforeCreate(ODocument iDocument) {
      beforeCreate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterCreate(ODocument iDocument) {
      afterCreate++;
    }

    @Override
    public RESULT onRecordBeforeUpdate(ODocument iDocument) {
      beforeUpdate++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterUpdate(ODocument iDocument) {
      afterUpdate++;
    }

    @Override
    public RESULT onRecordBeforeDelete(ODocument iDocument) {
      beforeDelete++;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterDelete(ODocument iDocument) {
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
