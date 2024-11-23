package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ODatabasePoolRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/poolRemote";
  private OServer server;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void testPoolCloseTx() {
    OxygenDB oxygenDb =
        new OxygenDB(
            "remote:localhost:",
            "root",
            "root",
            OxygenDBConfig.builder().addConfig(OGlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!oxygenDb.exists("test")) {
      oxygenDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(oxygenDb, "test", "admin", "admin");
    ODatabaseSessionInternal db = (ODatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new ODocument("Test"));
    db.close();
    db = (ODatabaseSessionInternal) pool.acquire();
    assertEquals(db.countClass("Test"), 0);

    pool.close();
    oxygenDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    OxygenDB oxygenDb =
        new OxygenDB(
            "embedded:",
            OxygenDBConfig.builder().addConfig(OGlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!oxygenDb.exists("test")) {
      oxygenDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(oxygenDb, "test", "admin", "admin");
    var db = pool.acquire();
    db.close();
    pool.close();
    oxygenDb.close();
  }

  @After
  public void after() {

    server.shutdown();
    Oxygen.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Oxygen.instance().startup();
  }
}
