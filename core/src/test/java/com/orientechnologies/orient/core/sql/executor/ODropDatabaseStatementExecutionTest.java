package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    String dbName = "ODropDatabaseStatementExecutionTest_testPlain";
    OxygenDB oxygenDb =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
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

      ODatabaseSession session =
          oxygenDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();

      oxygenDb.execute("drop database " + dbName);
      Assert.assertFalse(oxygenDb.exists(dbName));
    } finally {
      oxygenDb.close();
    }
  }

  @Test
  public void testIfExists1() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists1";
    final OxygenDB oxygenDb =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
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

      ODatabaseSession session =
          oxygenDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();

      oxygenDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(oxygenDb.exists(dbName));
    } finally {
      oxygenDb.close();
    }
  }

  @Test
  public void testIfExists2() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists2";
    OxygenDB oxygenDb =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      oxygenDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(oxygenDb.exists(dbName));
    } finally {
      oxygenDb.close();
    }
  }
}
