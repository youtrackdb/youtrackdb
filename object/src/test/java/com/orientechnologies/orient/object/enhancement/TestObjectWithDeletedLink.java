package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestObjectWithDeletedLink {

  private OObjectDatabaseTxInternal db;

  @Before
  public void before() {
    db = new OObjectDatabaseTxInternal("memory:" + TestObjectWithDeletedLink.class.getSimpleName());
    db.create();
    db.getEntityManager().registerEntityClass(SimpleSelfRef.class);
  }

  @After
  public void after() {
    db.activateOnCurrentThread();
    db.drop();
  }

  @Test
  public void testDeletedLink() {
    db.activateOnCurrentThread();

    db.begin();
    SimpleSelfRef ob1 = new SimpleSelfRef();
    ob1.setName("hobby one ");
    SimpleSelfRef ob2 = new SimpleSelfRef();
    ob2.setName("2");
    ob1.setFriend(ob2);

    ob1 = db.save(ob1);
    db.commit();

    db.begin();
    ob1 = db.reload(ob1, "", true);
    ob2 = ob1.getFriend();
    Assert.assertNotNull(ob1.getFriend());
    db.delete(ob2);
    db.commit();

    ob1 = db.reload(ob1, "", true);
    Assert.assertNull(ob1.getFriend());
  }
}
