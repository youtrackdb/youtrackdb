package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionQueryIndexTests {

  private YouTrackDB youTrackDB;
  private YTDatabaseSessionInternal database;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        (YTDatabaseSessionInternal)
            youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    YTClass clazz = database.createClass("test");
    YTProperty prop = clazz.createProperty(database, "test", YTType.STRING);
    prop.createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);

    database.begin();
    YTEntityImpl doc = database.newInstance("test");
    doc.setProperty("test", "abcdefg");
    database.save(doc);
    YTResultSet res = database.query("select from Test where test='abcdefg' ");

    assertEquals(1L, res.stream().count());
    res.close();
    res = database.query("select from Test where test='aaaaa' ");

    assertEquals(0L, res.stream().count());
    res.close();
  }

  @Test
  public void test2() {
    YTClass clazz = database.createClass("Test2");
    clazz.createProperty(database, "foo", YTType.STRING);
    clazz.createProperty(database, "bar", YTType.STRING);
    clazz.createIndex(database, "Test2.foo_bar", YTClass.INDEX_TYPE.NOTUNIQUE, "foo", "bar");

    database.begin();
    YTEntityImpl doc = database.newInstance("Test2");
    doc.setProperty("foo", "abcdefg");
    doc.setProperty("bar", "abcdefg");
    database.save(doc);
    YTResultSet res = database.query("select from Test2 where foo='abcdefg' and bar = 'abcdefg' ");

    assertEquals(1L, res.stream().count());
    res.close();
    res = database.query("select from Test2 where foo='aaaaa' and bar = 'aaa'");

    assertEquals(0L, res.stream().count());
    res.close();
  }

  @After
  public void after() {
    database.close();
    youTrackDB.close();
  }
}
