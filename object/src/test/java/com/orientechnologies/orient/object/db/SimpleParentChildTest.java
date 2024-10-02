package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.entity.ObjectWithSet;
import com.orientechnologies.orient.object.db.entity.SimpleChild;
import com.orientechnologies.orient.object.db.entity.SimpleParent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 17/02/17. */
public class SimpleParentChildTest {

  private OObjectDatabaseTx database;

  String url = "memory:" + SimpleParentChildTest.class.getSimpleName();

  @Before
  public void before() {
    database = new OObjectDatabaseTx(url);
    database.create();
    database.getEntityManager().registerEntityClass(SimpleChild.class);
    database.getEntityManager().registerEntityClass(SimpleParent.class);
    database.getEntityManager().registerEntityClass(ObjectWithSet.class);
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testParentChild() {
    database.begin();
    SimpleChild sc = new SimpleChild();
    sc.setName("aa");
    SimpleParent sa = new SimpleParent();
    sa.setChild(sc);
    SimpleParent ret = database.save(sa);
    database.commit();

    database.getLocalCache().clear();
    ODocument doc = ((OObjectDatabaseTx) database).getUnderlying().load(ret.getId().getIdentity());
    assertEquals(doc.fieldType("child"), OType.LINK);
  }

  @Test
  public void testWithSets() {
    database.begin();
    ObjectWithSet parent = new ObjectWithSet();
    ObjectWithSet child = new ObjectWithSet();
    parent.addFriend(child);
    child.setName("child1");
    ObjectWithSet savedParent = database.save(parent);
    database.commit();

    String parentId = savedParent.getId();

    this.database.close();
    this.database = new OObjectDatabaseTx(url);
    this.database.open("admin", "admin");

    database.begin();
    ObjectWithSet retrievedParent = this.database.load(new ORecordId(parentId));
    ObjectWithSet retrievedChild = retrievedParent.getFriends().iterator().next();
    retrievedChild.setName("child2");
    this.database.save(retrievedParent);
    database.commit();

    this.database.close();
    this.database = new OObjectDatabaseTx(url);
    this.database.open("admin", "admin");

    retrievedParent = this.database.load(new ORecordId(parentId));
    Assert.assertEquals("child2", retrievedParent.getFriends().iterator().next().getName());
  }
}
