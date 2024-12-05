package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class YTClassSecurityTest {

  private static final String DB_NAME = YTClassSecurityTest.class.getSimpleName();
  private static YouTrackDB orient;
  private YTDatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    orient =
        new YouTrackDB(
            "plocal:.",
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.execute(
        "create database "
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin, reader identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role reader, writer identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role writer)");
    this.db = (YTDatabaseSessionInternal) orient.open(DB_NAME, "admin",
        OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testReadWithClassPermissions() {
    db.createClass("Person");
    db.begin();
    ORole reader = db.getMetadata().getSecurity().getRole("reader");
    reader.grant(db, ORule.ResourceGeneric.CLASS, "Person", ORole.PERMISSION_NONE);
    reader.revoke(db, ORule.ResourceGeneric.CLASS, "Person", ORole.PERMISSION_READ);
    reader.save(db);
    db.commit();

    db.begin();
    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);
    db.commit();

    db.close();

    db = (YTDatabaseSessionInternal) orient.open(DB_NAME, "reader",
        OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    try (final YTResultSet resultSet = db.query("SELECT from Person")) {
      Assert.assertFalse(resultSet.hasNext());
    }
  }
}
