package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.exception.YTSecurityException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ORemoteSecurityTests {

  private static final String DB_NAME = ORemoteSecurityTests.class.getSimpleName();
  private YouTrackDB orient;
  private OServer server;
  private YTDatabaseSession db;

  @Before
  public void before()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    server = OServer.startFromClasspathConfig("abstract-orientdb-server-config.xml");
    orient = new YouTrackDB("remote:localhost", "root", "root", YouTrackDBConfig.defaultConfig());
    orient.execute(
        "create database ? memory users (admin identified by 'admin' role admin, writer identified"
            + " by 'writer' role writer, reader identified by 'reader' role reader)",
        DB_NAME);
    this.db = orient.open(DB_NAME, "admin", "admin");
    YTClass person = db.createClass("Person");
    person.createProperty(db, "name", YTType.STRING);
  }

  @After
  public void after() {
    this.db.activateOnCurrentThread();
    this.db.close();
    orient.drop(DB_NAME);
    orient.close();
    server.shutdown();
  }

  @Test
  public void testCreate() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET create = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        elem = filteredSession.newElement("Person");
        elem.setProperty("name", "bar");
        filteredSession.save(elem);
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }
    }
  }

  @Test
  public void testSqlCreate() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET create = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      filteredSession.command("insert into Person SET name = 'foo'");
      filteredSession.commit();
      try {
        filteredSession.begin();
        filteredSession.command("insert into Person SET name = 'bar'");
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }
    }
  }

  @Test
  public void testSqlRead() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    db.close();
    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person")) {
        Assert.assertTrue(rs.hasNext());
        rs.next();
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlReadWithIndex() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person where name = 'bar'")) {

        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlReadWithIndex2() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (surname = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "bar");
    db.save(elem);
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person where name = 'foo'")) {
        Assert.assertTrue(rs.hasNext());
        OResult item = rs.next();
        Assert.assertEquals("foo", item.getProperty("surname"));
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testBeforeUpdateCreate() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET BEFORE UPDATE = (name = 'bar')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        elem = filteredSession.bindToSession(elem);
        elem.setProperty("name", "baz");
        filteredSession.save(elem);
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }
      Assert.assertEquals("foo", filteredSession.bindToSession(elem).getProperty("name"));
    }
  }

  @Test
  public void testBeforeUpdateCreateSQL() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET BEFORE UPDATE = (name = 'bar')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        filteredSession.command("update Person set name = 'bar'");
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }

      Assert.assertEquals("foo", filteredSession.bindToSession(elem).getProperty("name"));
    }
  }

  @Test
  public void testAfterUpdate() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET AFTER UPDATE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        elem = filteredSession.bindToSession(elem);
        elem.setProperty("name", "bar");
        filteredSession.save(elem);
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }

      Assert.assertEquals("foo", filteredSession.bindToSession(elem).getProperty("name"));
    }
  }

  @Test
  public void testAfterUpdateSQL() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET AFTER UPDATE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        filteredSession.command("update Person set name = 'bar'");
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }

      Assert.assertEquals("foo", filteredSession.bindToSession(elem).getProperty("name"));
    }
  }

  @Test
  public void testDelete() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET DELETE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {

      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "bar");
      filteredSession.save(elem);
      filteredSession.commit();
      try {
        filteredSession.begin();
        elem = filteredSession.bindToSession(elem);
        filteredSession.delete(elem);
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }

      filteredSession.begin();
      elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.delete(elem);
      filteredSession.commit();
    }
  }

  @Test
  public void testDeleteSQL() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET DELETE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.begin();
      YTEntity elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);

      elem = filteredSession.newElement("Person");
      elem.setProperty("name", "bar");
      filteredSession.save(elem);
      filteredSession.commit();

      filteredSession.begin();
      filteredSession.command("delete from Person where name = 'foo'");
      filteredSession.commit();
      try {
        filteredSession.begin();
        filteredSession.command("delete from Person where name = 'bar'");
        filteredSession.commit();
        Assert.fail();
      } catch (YTSecurityException ex) {
      }

      try (OResultSet rs = filteredSession.query("select from Person")) {
        Assert.assertTrue(rs.hasNext());
        Assert.assertEquals("bar", rs.next().getProperty("name"));
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlCount() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select count(*) as count from Person")) {
        Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
      }
    }
  }

  @Test
  public void testSqlCountWithIndex() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    db.close();
    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs =
          filteredSession.query("select count(*) as count from Person where name = 'bar'")) {
        Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
      }

      try (OResultSet rs =
          filteredSession.query("select count(*) as count from Person where name = 'foo'")) {
        Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
      }
    }
  }

  @Test
  public void testIndexGet() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "bar")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }

      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "foo")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
    }
  }

  @Test
  public void testIndexGetAndColumnSecurity() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    db.commit();

    try (YTDatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "bar")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }

      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "foo")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
    }
  }

  @Test
  public void testReadHiddenColumn() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);
    db.commit();

    db.close();

    db = orient.open(DB_NAME, "reader", "reader");
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      OResult item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
    }
  }

  @Test
  public void testUpdateHiddenColumn() {
    db.begin();
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");
    db.commit();

    db.begin();
    YTEntity elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);
    db.commit();

    db.close();

    db = orient.open(DB_NAME, "reader", "reader");
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      try {
        db.begin();
        OResult item = resultSet.next();
        YTEntity doc = item.getElement().get();
        doc.setProperty("name", "bar");

        doc.save();
        db.commit();
        Assert.fail();
      } catch (Exception e) {

      }
    }
  }
}
