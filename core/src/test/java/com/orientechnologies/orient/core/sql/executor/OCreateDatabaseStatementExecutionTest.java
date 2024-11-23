package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    final String dbName = "OCreateDatabaseStatementExecutionTest_testPlain";

    final OxygenDB oxygenDb =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try (OResultSet result =
        oxygenDb.execute(
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
    Assert.assertTrue(oxygenDb.exists(dbName));

    try {
      final ODatabaseSession session =
          oxygenDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();
    } finally {
      oxygenDb.drop(dbName);
      oxygenDb.close();
    }
  }

  @Test
  public void testNoDefaultUsers() {
    String dbName = "OCreateDatabaseStatementExecutionTest_testNoDefaultUsers";
    OxygenDB oxygenDb = new OxygenDB("embedded:./target/", OxygenDBConfig.defaultConfig());
    try (OResultSet result =
        oxygenDb.execute(
            "create database "
                + dbName
                + " plocal {'config':{'security.createDefaultUsers': false}}")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(oxygenDb.exists(dbName));

    try {
      ODatabaseSession session =
          oxygenDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      Assert.fail();
    } catch (OSecurityAccessException e) {
    } finally {
      oxygenDb.drop(dbName);
      oxygenDb.close();
    }
  }
}
