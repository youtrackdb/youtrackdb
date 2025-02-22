package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionTest {

  private YouTrackDB youTrackDB;
  private DatabaseSession db;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase("test", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    db = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    db.begin();
    var v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.commit();

    db.begin();
    v = db.bindToSession(v);
    v.setProperty("name", "Bar");
    db.rollback();

    v = db.bindToSession(v);
    Assert.assertEquals("Foo", v.getProperty("name"));
  }

  @After
  public void after() {
    db.close();
    youTrackDB.close();
  }
}
