package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.YouTrackDBManager;
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
    YouTrackDB youTrackDb =
        new YouTrackDB(
            "remote:localhost:",
            "root",
            "root",
            YouTrackDBConfig.builder().addConfig(OGlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(youTrackDb, "test", "admin", "admin");
    ODatabaseSessionInternal db = (ODatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new ODocument("Test"));
    db.close();
    db = (ODatabaseSessionInternal) pool.acquire();
    assertEquals(0, db.countClass("Test"));

    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder().addConfig(OGlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(youTrackDb, "test", "admin", "admin");
    var db = pool.acquire();
    db.close();
    pool.close();
    youTrackDb.close();
  }

  @After
  public void after() {

    server.shutdown();
    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }
}
