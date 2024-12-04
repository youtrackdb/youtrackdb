package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    String dbName = "ODropDatabaseStatementExecutionTest_testPlain";
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      try (OResultSet result =
          youTrackDb.execute(
              "create database "
                  + dbName
                  + " plocal"
                  + " users ( admin identified by '"
                  + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
                  + "' role admin)")) {
        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        Assert.assertEquals(true, item.getProperty("created"));
      }
      Assert.assertTrue(youTrackDb.exists(dbName));

      ODatabaseSession session =
          youTrackDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();

      youTrackDb.execute("drop database " + dbName);
      Assert.assertFalse(youTrackDb.exists(dbName));
    } finally {
      youTrackDb.close();
    }
  }

  @Test
  public void testIfExists1() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists1";
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      try (OResultSet result =
          youTrackDb.execute(
              "create database "
                  + dbName
                  + " plocal"
                  + " users ( admin identified by '"
                  + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
                  + "' role admin)")) {
        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        Assert.assertEquals(true, item.getProperty("created"));
      }
      Assert.assertTrue(youTrackDb.exists(dbName));

      ODatabaseSession session =
          youTrackDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();

      youTrackDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(youTrackDb.exists(dbName));
    } finally {
      youTrackDb.close();
    }
  }

  @Test
  public void testIfExists2() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists2";
    try (YouTrackDB youTrackDb = new YouTrackDB(
        DBTestBase.embeddedDBUrl(getClass()) + getClass().getSimpleName(),
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build())) {
      youTrackDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(youTrackDb.exists(dbName));
    }
  }
}
