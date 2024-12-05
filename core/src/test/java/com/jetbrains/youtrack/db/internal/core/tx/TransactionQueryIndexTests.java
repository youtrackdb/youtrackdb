package com.jetbrains.youtrack.db.internal.core.tx;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
    EntityImpl doc = database.newInstance("test");
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
    EntityImpl doc = database.newInstance("Test2");
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
