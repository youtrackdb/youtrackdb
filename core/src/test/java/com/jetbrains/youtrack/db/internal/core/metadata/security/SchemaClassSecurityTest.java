package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SchemaClassSecurityTest {

  private static final String DB_NAME = SchemaClassSecurityTest.class.getSimpleName();
  private static YouTrackDB youTrackDB;
  private DatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    youTrackDB =
        new YouTrackDBImpl(
            "plocal:.",
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.close();
  }

  @Before
  public void before() {
    youTrackDB.execute(
        "create database "
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin, reader identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role reader, writer identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role writer)");
    this.db = (DatabaseSessionInternal) youTrackDB.open(DB_NAME, "admin",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    youTrackDB.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testReadWithClassPermissions() {
    db.createClass("Person");
    db.begin();
    var reader = db.getMetadata().getSecurity().getRole("reader");
    reader.grant(db, Rule.ResourceGeneric.CLASS, "Person", Role.PERMISSION_NONE);
    reader.revoke(db, Rule.ResourceGeneric.CLASS, "Person", Role.PERMISSION_READ);
    reader.save(db);
    db.commit();

    db.begin();
    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.commit();

    db.close();

    db = (DatabaseSessionInternal) youTrackDB.open(DB_NAME, "reader",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    try (final var resultSet = db.query("SELECT from Person")) {
      Assert.assertFalse(resultSet.hasNext());
    }
  }
}
