package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

public class ODatabasePoolTest {

  @Test
  public void testPool() {
    final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabasePool pool =
        new ODatabasePool(oxygenDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    var db = (ODatabaseSessionInternal) pool.acquire();
    db.executeInTx(() -> db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId())));
    db.close();
    pool.close();
    oxygenDb.close();
  }

  @Test
  public void testPoolCloseTx() {
    final OxygenDB oxygenDb =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!oxygenDb.exists("test")) {
      oxygenDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(oxygenDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabaseSessionInternal db = (ODatabaseSessionInternal) pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new ODocument("Test"));
    db.close();
    db = (ODatabaseSessionInternal) pool.acquire();
    assertEquals(db.countClass("Test"), 0);
    db.close();
    pool.close();
    oxygenDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    final OxygenDB oxygenDb =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!oxygenDb.exists("test")) {
      oxygenDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(oxygenDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    var db = pool.acquire();
    db.close();
    pool.close();
    oxygenDb.close();
  }
}
