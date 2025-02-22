package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateSecurityTest {

  private static final String DB_NAME = PredicateSecurityTest.class.getSimpleName();
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
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    youTrackDB.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testCreate() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });
    try {
      db.executeInTx(
          () -> {
            var elem = db.newEntity("Person");
            elem.setProperty("name", "bar");
          });

      Assert.fail();
    } catch (SecurityException ex) {
    }
  }

  @Test
  public void testSqlCreate() throws InterruptedException {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

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
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    var rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    var rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex2() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "surname = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    var rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    var item = rs.next();
    Assert.assertEquals("foo", item.getProperty("surname"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBeforeUpdateCreate() throws InterruptedException {
    var security = db.getSharedContext().getSecurity();
    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    var elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              return e;
            });

    try {
      db.begin();
      elem = db.bindToSession(elem);
      elem.setProperty("name", "baz");
      var elemToSave = elem;
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {

    }

    Assert.assertFalse(db.isTxActive());
    elem = db.load(elem.getIdentity());
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testBeforeUpdateCreateSQL() throws InterruptedException {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
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
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    var elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
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
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    var elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
              return e;
            });

    try {
      db.begin();
      elem = db.bindToSession(elem);
      elem.setProperty("name", "bar");
      var elemToSave = elem;
      db.commit();
      Assert.fail();
    } catch (SecurityException ex) {
    }

    elem = db.load(elem.getIdentity());
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testAfterUpdateSQL() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    var elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "foo");
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
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    var elem =
        db.computeInTx(
            () -> {
              var e = db.newEntity("Person");
              e.setProperty("name", "bar");
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
              return e;
            });

    var elemToDelete = elem;
    db.executeInTx(() -> db.delete(db.bindToSession(elemToDelete)));
  }

  @Test
  public void testDeleteSQL() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
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

    var rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    Assert.assertEquals("bar", rs.next().getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlCount() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    var rs = db.query("select count(*) as count from Person");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testSqlCountWithIndex() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var e = db.newEntity("Person");
          e.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    var rs = db.query("select count(*) as count from Person where name = 'bar'");
    Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
    rs.close();

    rs = db.query("select count(*) as count from Person where name = 'foo'");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testIndexGet() {
    var security = db.getSharedContext().getSecurity();

    var person = db.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "foo");
        });

    db.executeInTx(
        () -> {
          var elem = db.newEntity("Person");
          elem.setProperty("name", "bar");
        });

    db.close();
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"

    var index = db.getMetadata().getIndexManager().getIndex("Person.name");

    try (var rids = index.getInternal().getRids(db, "bar")) {
      Assert.assertEquals(0, rids.count());
    }

    try (var rids = index.getInternal().getRids(db, "foo")) {
      Assert.assertEquals(1, rids.count());
    }
  }
}
