package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateSecurityTest {

  private static final String DB_NAME = PredicateSecurityTest.class.getSimpleName();
  private static OxygenDB orient;
  private ODatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    orient =
        new OxygenDB(
            "plocal:.",
            OxygenDBConfig.builder()
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
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testCreate() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });
    try {
      db.executeInTx(
          () -> {
            var elem = db.newElement("Person");
            elem.setProperty("name", "bar");
            db.save(elem);
          });

      Assert.fail();
    } catch (OSecurityException ex) {
    }
  }

  @Test
  public void testSqlCreate() throws InterruptedException {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setCreateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.begin();
    db.command("insert into Person SET name = 'foo'");
    db.commit();

    try {
      db.begin();
      db.command("insert into Person SET name = 'bar'");
      db.commit();
      Assert.fail();
    } catch (OSecurityException ex) {
    }
  }

  @Test
  public void testSqlRead() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty(db, "name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex2() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty(db, "name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "surname = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          elem.setProperty("surname", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals("foo", item.getProperty("surname"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBeforeUpdateCreate() throws InterruptedException {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setBeforeUpdateRule(db, "name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    Thread.sleep(500);
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
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
    } catch (OSecurityException ex) {
    }

    elem = db.load(elem.getIdentity());

    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testBeforeUpdateCreateSQL() throws InterruptedException {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      db.command("update Person set name = 'bar'");
      db.commit();
      return false;
    } catch (OSecurityException ex) {
    }

    Assert.assertEquals("foo", db.bindToSession(elem).getProperty("name"));
    return true;
  }

  @Test
  public void testAfterUpdate() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
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
    } catch (OSecurityException ex) {
    }

    elem = db.load(elem.getIdentity());
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testAfterUpdateSQL() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setAfterUpdateRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    try {
      db.begin();
      db.command("update Person set name = 'bar'");
      db.commit();
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    Assert.assertEquals("foo", db.bindToSession(elem).getProperty("name"));
  }

  @Test
  public void testDelete() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
              e.setProperty("name", "bar");
              db.save(e);
              return e;
            });

    try {
      var elemToDelete = elem;
      db.executeInTx(() -> db.delete(db.bindToSession(elemToDelete)));
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    elem =
        db.computeInTx(
            () -> {
              var e = db.newElement("Person");
              e.setProperty("name", "foo");
              db.save(e);
              return e;
            });

    var elemToDelete = elem;
    db.executeInTx(() -> db.delete(db.bindToSession(elemToDelete)));
  }

  @Test
  public void testDeleteSQL() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setDeleteRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);
    db.commit();

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
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
    } catch (OSecurityException ex) {
    }

    OResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    Assert.assertEquals("bar", rs.next().getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlCount() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty(db, "name", OType.STRING);

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select count(*) as count from Person");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testSqlCountWithIndex() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty(db, "name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          var e = db.newElement("Person");
          e.setProperty("name", "foo");
          db.save(e);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select count(*) as count from Person where name = 'bar'");
    Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
    rs.close();

    rs = db.query("select count(*) as count from Person where name = 'foo'");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testIndexGet() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty(db, "name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.executeInTx(
        () -> {
          OElement elem = db.newElement("Person");
          elem.setProperty("name", "foo");
          db.save(elem);
        });

    db.executeInTx(
        () -> {
          var elem = db.newElement("Person");
          elem.setProperty("name", "bar");
          db.save(elem);
        });

    db.close();
    this.db =
        (ODatabaseSessionInternal)
            orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"

    OIndex index = db.getMetadata().getIndexManager().getIndex("Person.name");

    try (Stream<ORID> rids = index.getInternal().getRids(db, "bar")) {
      Assert.assertEquals(0, rids.count());
    }

    try (Stream<ORID> rids = index.getInternal().getRids(db, "foo")) {
      Assert.assertEquals(1, rids.count());
    }
  }
}
