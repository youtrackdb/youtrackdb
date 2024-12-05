package com.orientechnologies.core.tx;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.record.YTVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionTest {

  private YouTrackDB youTrackDB;
  private YTDatabaseSession db;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    db = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    db.begin();
    YTVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();

    db.begin();
    v = db.bindToSession(v);
    v.setProperty("name", "Bar");
    db.save(v);
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
