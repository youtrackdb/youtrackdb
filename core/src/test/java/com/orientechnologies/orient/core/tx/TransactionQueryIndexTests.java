package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionQueryIndexTests {

  private OxygenDB oxygenDB;
  private ODatabaseSessionInternal database;

  @Before
  public void before() {
    oxygenDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        (ODatabaseSessionInternal)
            oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    OClass clazz = database.createClass("test");
    OProperty prop = clazz.createProperty(database, "test", OType.STRING);
    prop.createIndex(database, OClass.INDEX_TYPE.NOTUNIQUE);

    database.begin();
    ODocument doc = database.newInstance("test");
    doc.setProperty("test", "abcdefg");
    database.save(doc);
    OResultSet res = database.query("select from Test where test='abcdefg' ");

    assertEquals(1L, res.stream().count());
    res.close();
    res = database.query("select from Test where test='aaaaa' ");

    assertEquals(0L, res.stream().count());
    res.close();
  }

  @Test
  public void test2() {
    OClass clazz = database.createClass("Test2");
    clazz.createProperty(database, "foo", OType.STRING);
    clazz.createProperty(database, "bar", OType.STRING);
    clazz.createIndex(database, "Test2.foo_bar", OClass.INDEX_TYPE.NOTUNIQUE, "foo", "bar");

    database.begin();
    ODocument doc = database.newInstance("Test2");
    doc.setProperty("foo", "abcdefg");
    doc.setProperty("bar", "abcdefg");
    database.save(doc);
    OResultSet res = database.query("select from Test2 where foo='abcdefg' and bar = 'abcdefg' ");

    assertEquals(1L, res.stream().count());
    res.close();
    res = database.query("select from Test2 where foo='aaaaa' and bar = 'aaa'");

    assertEquals(0L, res.stream().count());
    res.close();
  }

  @After
  public void after() {
    database.close();
    oxygenDB.close();
  }
}
