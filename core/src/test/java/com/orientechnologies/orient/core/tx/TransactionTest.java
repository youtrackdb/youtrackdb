package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionTest {

  private OxygenDB oxygenDB;
  private ODatabaseSession db;

  @Before
  public void before() {
    oxygenDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    db = oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    oxygenDB.close();
  }
}
