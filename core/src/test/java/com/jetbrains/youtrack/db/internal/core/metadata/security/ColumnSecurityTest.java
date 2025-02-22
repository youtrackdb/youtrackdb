package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ColumnSecurityTest {

  static String DB_NAME = "test";
  static YouTrackDB context;
  private DatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    context =
        new YouTrackDBImpl(
            "plocal:.",
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    context.close();
  }

  @Before
  public void before() {
    context.execute(
        "create database "
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "admin",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    context.drop("test");
    this.db = null;
  }

  @Test
  public void testIndexWithPolicy() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy1() throws InterruptedException {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "surname", PropertyType.STRING);

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Thread.sleep(100);
    try {
      db.command("create index Person.name_surname on Person (name, surname) NOTUNIQUE");
      Assert.fail();
    } catch (IndexException e) {

    }
  }

  @Test
  public void testIndexWithPolicy2() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    policy.setBeforeUpdateRule(db, "name = 'foo'");
    policy.setAfterUpdateRule(db, "name = 'foo'");
    policy.setDeleteRule(db, "name = 'foo'");

    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy3() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy4() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "address", PropertyType.STRING);

    db.command("create index Person.name_address on Person (name, address) NOTUNIQUE");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);
  }

  @Test
  public void testIndexWithPolicy5() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "surname", PropertyType.STRING);

    db.command("create index Person.name_surname on Person (name, surname) NOTUNIQUE");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);

    try {
      security.setSecurityPolicy(
          db, security.getRole(db, "reader"), "database.class.Person.name", policy);
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testIndexWithPolicy6() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);
  }

  @Test
  public void testReadFilterOneProperty() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    var rs = db.query("select from Person");
    var fooFound = false;
    var nullFound = false;

    for (var i = 0; i < 2; i++) {
      var item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if (item.getProperty("name") == null) {
        nullFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(nullFound);
  }

  @Test
  public void testReadFilterOnePropertyWithIndex() {
    var security = db.getSharedContext().getSecurity();

    var clazz = db.createClass("Person");
    clazz.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    var rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    Assert.assertTrue(
        rs.getExecutionPlan().getSteps().stream()
            .anyMatch(x -> x instanceof FetchFromIndexStep));
    rs.close();
  }

  @Test
  public void testReadWithPredicateAndQuery() throws InterruptedException {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name IN (select 'foo' as foo)");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");

    var rs = db.query("select from Person");
    var fooFound = false;
    var barFound = false;

    for (var i = 0; i < 2; i++) {
      var item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if ("bar".equals(item.getProperty("name"))) {
        barFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(barFound);

    db.close();
    Thread.sleep(200);

    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    rs = db.query("select from Person");
    fooFound = false;
    var nullFound = false;

    for (var i = 0; i < 2; i++) {
      var item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if (item.getProperty("name") == null) {
        nullFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(nullFound);
  }

  @Test
  public void testReadFilterOnePropertyWithQuery() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");

    db.close();

    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    var rs = db.query("select from Person where name = 'foo' OR name = 'bar'");

    var item = rs.next();
    Assert.assertEquals("foo", item.getProperty("name"));

    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testCreate() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    try {
      Assert.fail();
    } catch (SecurityException e) {

    }
  }

  @Test
  public void testBeforeUpdate() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");

    db.close();

    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (var rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try {
      db.command("UPDATE Person SET name = 'bar1' WHERE name = 'bar'");
      Assert.fail();
    } catch (SecurityException e) {

    }
  }

  @Test
  public void testAfterUpdate() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name <> 'invalid'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (var rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try {
      db.command("UPDATE Person SET name = 'invalid'");
      Assert.fail();
    } catch (SecurityException e) {

    }
  }

  @Test
  public void testReadHiddenColumn() {
    db.command("CREATE CLASS Person");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    db.close();

    db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    try (final var resultSet = db.query("SELECT from Person")) {
      var item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
    }
  }

  @Test
  public void testUpdateHiddenColumn() {

    db.command("CREATE CLASS Person");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    var elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");

    db.close();

    db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    try (final var resultSet = db.query("SELECT from Person")) {
      var item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
      var doc = item.castToEntity();
      doc.setProperty("name", "bar");
      try {
        Assert.fail();
      } catch (Exception e) {

      }
    }
  }
}
