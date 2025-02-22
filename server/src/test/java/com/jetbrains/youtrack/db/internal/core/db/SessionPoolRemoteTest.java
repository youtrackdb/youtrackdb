package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.session.SessionPool;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SessionPoolRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/poolRemote";
  private YouTrackDBServer server;

  @Before
  public void before() throws Exception {
    GlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/jetbrains/youtrack/db/internal/server/network/youtrackdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void testPoolCloseTx() {
    YouTrackDBImpl youTrackDb =
        new YouTrackDBImpl(
            "remote:localhost:",
            "root",
            "root",
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    SessionPool pool = new SessionPoolImpl(youTrackDb, "test", "admin", "admin");
    DatabaseSessionInternal db = (DatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new EntityImpl("Test"));
    db.close();
    db = (DatabaseSessionInternal) pool.acquire();
    assertEquals(0, db.countClass("Test"));

    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    YouTrackDBImpl youTrackDb =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.DB_POOL_MAX, 1).build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
    }

    SessionPool pool = new SessionPoolImpl(youTrackDb, "test", "admin", "admin");
    var db = pool.acquire();
    db.close();
    pool.close();
    youTrackDb.close();
  }

  @After
  public void after() {

    server.shutdown();
    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }
}
