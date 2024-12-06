package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateSecurityTest {

  private static final String DB_NAME = PredicateSecurityTest.class.getSimpleName();
  private static YouTrackDB orient;
  private DatabaseSessionInternal db;

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
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin, reader identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role reader, writer identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role writer)");
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testCreate() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });
    try {
      db.executeInTx(
          () -> {
            var elem = db.newEntity("Person");
            elem.setProperty("name", "bar");
            db.save(elem);
          });

      Assert.fail();
    } catch (SecurityException ex) {
    }
  }

  @Test
  public void testSqlCreate() throws InterruptedException {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.begin();
    db.command("insert into Person SET name = 'foo'");
    db.commit();

    try {
      db.begin();
      db.command("insert into Person SET name = 'bar'");
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }
  }

  @Test
  public void testSqlRead() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    ResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    ResultSet rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex2() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "surname = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    ResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    Result item = rs.next();
    Assert.assertEquals("foo", item.getProperty("surname"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBeforeUpdateCreate() throws InterruptedException {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    Entity elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      elem = db.bindToSession(elem);
      elem.setProperty("name", "baz");
      var elemToSave = elem;
      db.save(elemToSave);
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }

    elem = db.load(elem.getIdentity());

    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testBeforeUpdateCreateSQL() throws InterruptedException {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();

    if (!doTestBeforeUpdateSQL()) {
      db.close();
      Thread.sleep(500);
      if (!doTestBeforeUpdateSQL()) {
        Assert.fail();
      }
    }
  }

  private boolean doTestBeforeUpdateSQL() {
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    Entity elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      db.command("update Person set name = 'bar'");
      db.commit();
      return false;
    } catch (SecurityException ex) {
    }

    Assert.assertEquals("foo", db.bindToSession(elem).getProperty("name"));
    return true;
  }

  @Test
  public void testAfterUpdate() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    Entity elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      elem = db.bindToSession(elem);
      elem.setProperty("name", "bar");
      var elemToSave = elem;
      db.save(elemToSave);
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }

    elem = db.load(elem.getIdentity());
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testAfterUpdateSQL() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    Entity elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      db.command("update Person set name = 'bar'");
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }

    Assert.assertEquals("foo", db.bindToSession(elem).getProperty("name"));
  }

  @Test
  public void testDelete() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    Entity elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "bar");
              db.save(e);
              return e;
            });

    try {
      var elemToDelete = elem;
      db.executeInTx(() -> db.delete(db.bindToSession(elemToDelete)));
      Assert.fail();
    } catch (SecurityException ex) {
    }

    elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    var elemToDelete = elem;
    db.executeInTx(() -> db.delete(db.bindToSession(elemToDelete)));
  }

  @Test
  public void testDeleteSQL() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.begin();
    db.command("delete from Person where name = 'foo'");
    db.commit();
    try {
      db.begin();
      db.command("delete from Person where name = 'bar'");
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }

    ResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    Assert.assertEquals("bar", rs.next().getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlCount() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    ResultSet rs = db.query("select count(*) as count from Person");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testSqlCountWithIndex() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var e = db.newEntity("Person");
          e.setProperty("name", "foo");
          db.save(e);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    ResultSet rs = db.query("select count(*) as count from Person where name = 'bar'");
    Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
    rs.close();

    rs = db.query("select count(*) as count from Person where name = 'foo'");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testIndexGet() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    SchemaClass person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          Entity elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            orient.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"

    Index index = db.getMetadata().getIndexManager().getIndex("Person.name");

    try (Stream<RID> rids = index.getInternal().getRids(db, "bar")) {
      Assert.assertEquals(0, rids.count());
    }

    try (Stream<RID> rids = index.getInternal().getRids(db, "foo")) {
      Assert.assertEquals(1, rids.count());
    }
  }
}
