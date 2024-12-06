package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
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
        new YouTrackDB(
            "plocal:.",
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy1() throws InterruptedException {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "surname", PropertyType.STRING);

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy4() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "address", PropertyType.STRING);

    db.command("create index Person.name_address on Person (name, address) NOTUNIQUE");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);
  }

  @Test
  public void testIndexWithPolicy5() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    person.createProperty(db, "surname", PropertyType.STRING);

    db.command("create index Person.name_surname on Person (name, surname) NOTUNIQUE");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);
  }

  @Test
  public void testReadFilterOneProperty() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    ResultSet rs = db.query("select from Person");
    boolean fooFound = false;
    boolean nullFound = false;

    for (int i = 0; i < 2; i++) {
      Result item = rs.next();
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass clazz = db.createClass("Person");
    clazz.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    ResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    Assert.assertTrue(
        rs.getExecutionPlan().get().getSteps().stream()
            .anyMatch(x -> x instanceof FetchFromIndexStep));
    rs.close();
  }

  @Test
  public void testReadWithPredicateAndQuery() throws InterruptedException {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name IN (select 'foo' as foo)");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    ResultSet rs = db.query("select from Person");
    boolean fooFound = false;
    boolean barFound = false;

    for (int i = 0; i < 2; i++) {
      Result item = rs.next();
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
    boolean nullFound = false;

    for (int i = 0; i < 2; i++) {
      Result item = rs.next();
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    db.save(elem);

    db.close();

    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    ResultSet rs = db.query("select from Person where name = 'foo' OR name = 'bar'");

    Result item = rs.next();
    Assert.assertEquals("foo", item.getProperty("name"));

    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testCreate() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    try {
      db.save(elem);
      Assert.fail();
    } catch (SecurityException e) {

    }
  }

  @Test
  public void testBeforeUpdate() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newEntity("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    db.save(elem);

    db.close();

    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (ResultSet rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
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
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name <> 'invalid'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();
    this.db = (DatabaseSessionInternal) context.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (ResultSet rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
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

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();

    db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    try (final ResultSet resultSet = db.query("SELECT from Person")) {
      Result item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
    }
  }

  @Test
  public void testUpdateHiddenColumn() {

    db.command("CREATE CLASS Person");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    Entity elem = db.newEntity("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();

    db = (DatabaseSessionInternal) context.open(DB_NAME, "reader", "reader");
    try (final ResultSet resultSet = db.query("SELECT from Person")) {
      Result item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
      Entity doc = item.getEntity().get();
      doc.setProperty("name", "bar");
      try {
        doc.save();
        Assert.fail();
      } catch (Exception e) {

      }
    }
  }
}
