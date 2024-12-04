package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OClassSecurityTest {

  private static final String DB_NAME = OClassSecurityTest.class.getSimpleName();
  private static YouTrackDB orient;
  private ODatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    orient =
        new YouTrackDB(
            "plocal:.",
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
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
    this.db = (ODatabaseSessionInternal) orient.open(DB_NAME, "admin",
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
    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);
    db.commit();

    db.close();

    db = (ODatabaseSessionInternal) orient.open(DB_NAME, "reader",
        OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      Assert.assertFalse(resultSet.hasNext());
    }
  }
}
