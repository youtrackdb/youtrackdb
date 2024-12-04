package com.orientechnologies.orient.core.tx;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionTest {

  private YouTrackDB youTrackDB;
  private ODatabaseSession db;

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
    OVertex v = db.newVertex("V");
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
