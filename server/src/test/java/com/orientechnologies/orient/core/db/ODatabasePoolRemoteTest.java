package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.io.OFileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
    GlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
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
            YouTrackDBConfig.builder().addConfig(GlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(youTrackDb, "test", "admin", "admin");
    YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new EntityImpl("Test"));
    db.close();
    db = (YTDatabaseSessionInternal) pool.acquire();
    assertEquals(0, db.countClass("Test"));

    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder().addConfig(GlobalConfiguration.DB_POOL_MAX, 1).build());

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
