package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.exception.YTSecurityAccessException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    final String dbName = "OCreateDatabaseStatementExecutionTest_testPlain";

    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try (YTResultSet result =
        youTrackDb.execute(
            "create database "
                + dbName
                + " plocal"
                + " users ( admin identified by '"
                + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
                + "' role admin)")) {
      Assert.assertTrue(result.hasNext());
      YTResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(youTrackDb.exists(dbName));

    try {
      final YTDatabaseSession session =
          youTrackDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();
    } finally {
      youTrackDb.drop(dbName);
      youTrackDb.close();
    }
  }

  @Test
  public void testNoDefaultUsers() {
    String dbName = "OCreateDatabaseStatementExecutionTest_testNoDefaultUsers";
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    try (YTResultSet result =
        youTrackDb.execute(
            "create database "
                + dbName
                + " plocal {'config':{'security.createDefaultUsers': false}}")) {
      Assert.assertTrue(result.hasNext());
      YTResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(youTrackDb.exists(dbName));

    try {
      YTDatabaseSession session =
          youTrackDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      Assert.fail();
    } catch (YTSecurityAccessException e) {
    } finally {
      youTrackDb.drop(dbName);
      youTrackDb.close();
    }
  }
}
