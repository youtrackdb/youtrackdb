package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class ODatabasePoolTest {

  @Test
  public void testPool() {
    final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabasePool pool =
        new ODatabasePool(youTrackDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    var db = (YTDatabaseSessionInternal) pool.acquire();
    db.executeInTx(
        () -> db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId())));
    db.close();
    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolCloseTx() {
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(youTrackDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new EntityImpl("Test"));
    db.close();
    db = (YTDatabaseSessionInternal) pool.acquire();
    assertEquals(db.countClass("Test"), 0);
    db.close();
    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(youTrackDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    var db = pool.acquire();
    db.close();
    pool.close();
    youTrackDb.close();
  }
}
