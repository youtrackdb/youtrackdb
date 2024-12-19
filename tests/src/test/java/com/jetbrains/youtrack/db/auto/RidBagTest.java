package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.StorageProxy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public abstract class RidBagTest extends BaseDBTest {

  @Parameters(value = "remote")
  public RidBagTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testAdd() throws Exception {
    RidBag bag = new RidBag(db);

    bag.add(new RecordId("#77:1"));
    Assert.assertTrue(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#78:2")));

    Iterator<Identifiable> iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    Identifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new RecordId("#77:1"));

    Assert.assertFalse(iterator.hasNext());
    assertEmbedded(bag.isEmbedded());
  }

  public void testAdd2() throws Exception {
    RidBag bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));

    Assert.assertTrue(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:3")));

    assertEquals(bag.size(), 2);
    assertEmbedded(bag.isEmbedded());
  }

  public void testAddRemoveInTheMiddleOfIteration() {
    RidBag bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    int counter = 0;
    Iterator<Identifiable> iterator = bag.iterator();

    bag.remove(new RecordId("#77:2"));
    while (iterator.hasNext()) {
      counter++;
      if (counter == 1) {
        bag.remove(new RecordId("#77:1"));
        bag.remove(new RecordId("#77:2"));
      }

      if (counter == 3) {
        bag.remove(new RecordId("#77:4"));
      }

      if (counter == 5) {
        bag.remove(new RecordId("#77:6"));
      }

      iterator.next();
    }

    Assert.assertTrue(bag.contains(new RecordId("#77:3")));
    Assert.assertTrue(bag.contains(new RecordId("#77:4")));
    Assert.assertTrue(bag.contains(new RecordId("#77:5")));

    Assert.assertFalse(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:6")));
    Assert.assertFalse(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#77:0")));

    assertEmbedded(bag.isEmbedded());

    final List<Identifiable> rids = new ArrayList<>();
    rids.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:5"));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);
    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    Assert.assertTrue(bag.contains(new RecordId("#77:3")));
    Assert.assertTrue(bag.contains(new RecordId("#77:4")));
    Assert.assertTrue(bag.contains(new RecordId("#77:5")));

    Assert.assertFalse(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:6")));
    Assert.assertFalse(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#77:0")));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemove() {
    RidBag bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    bag.remove(new RecordId("#77:1"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:4"));
    bag.remove(new RecordId("#77:6"));

    Assert.assertTrue(bag.contains(new RecordId("#77:3")));
    Assert.assertTrue(bag.contains(new RecordId("#77:4")));
    Assert.assertTrue(bag.contains(new RecordId("#77:5")));

    Assert.assertFalse(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:6")));
    Assert.assertFalse(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#77:0")));

    assertEmbedded(bag.isEmbedded());

    final List<Identifiable> rids = new ArrayList<Identifiable>();
    rids.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:5"));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);
    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    Assert.assertTrue(bag.contains(new RecordId("#77:3")));
    Assert.assertTrue(bag.contains(new RecordId("#77:4")));
    Assert.assertTrue(bag.contains(new RecordId("#77:5")));

    Assert.assertFalse(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:6")));
    Assert.assertFalse(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#77:0")));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveSBTreeContainsValues() {
    RidBag bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);
    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new RecordId("#77:1"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:4"));
    bag.remove(new RecordId("#77:6"));

    final List<Identifiable> rids = new ArrayList<>();
    rids.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:5"));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = ((EntityImpl) db.newEntity());
    RidBag otherBag = new RidBag(db);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);

    doc.save();
    db.commit();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveDuringIterationSBTreeContainsValues() {
    db.begin();
    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);

    doc.save();
    db.commit();

    RID rid = doc.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new RecordId("#77:1"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:2"));
    bag.remove(new RecordId("#77:4"));
    bag.remove(new RecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    final List<Identifiable> rids = new ArrayList<Identifiable>();
    rids.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:5"));

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    Iterator<Identifiable> iterator = bag.iterator();
    while (iterator.hasNext()) {
      final Identifiable identifiable = iterator.next();
      if (identifiable.equals(new RecordId("#77:4"))) {
        iterator.remove();
        assertTrue(rids.remove(identifiable));
      }
    }

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    assertEmbedded(bag.isEmbedded());
    doc = ((EntityImpl) db.newEntity());

    final RidBag otherBag = new RidBag(db);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);

    doc.save();
    db.commit();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testEmptyIterator() throws Exception {
    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());
    assertEquals(bag.size(), 0);

    for (Identifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    List<Identifiable> rids = new ArrayList<Identifiable>();

    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:5"));
    rids.add(new RecordId("#77:5"));

    bag.add(new RecordId("#77:6"));
    rids.add(new RecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);

    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();

    db.close();

    db = createSessionInstance();

    db.begin();
    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.remove(new RecordId("#77:4"));
    rids.remove(new RecordId("#77:4"));

    bag.remove(new RecordId("#77:4"));
    rids.remove(new RecordId("#77:4"));

    bag.remove(new RecordId("#77:2"));
    rids.remove(new RecordId("#77:2"));

    bag.remove(new RecordId("#77:2"));
    rids.remove(new RecordId("#77:2"));

    bag.remove(new RecordId("#77:7"));
    rids.remove(new RecordId("#77:7"));

    bag.remove(new RecordId("#77:8"));
    rids.remove(new RecordId("#77:8"));

    bag.remove(new RecordId("#77:8"));
    rids.remove(new RecordId("#77:8"));

    bag.remove(new RecordId("#77:8"));
    rids.remove(new RecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc.save();
    db.commit();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testContentChange() {
    EntityImpl entity = ((EntityImpl) db.newEntity());
    RidBag ridBag = new RidBag(db);
    entity.field("ridBag", ridBag);

    db.begin();
    entity.save();
    db.commit();

    db.begin();
    entity = db.bindToSession(entity);
    ridBag = entity.field("ridBag");
    ridBag.add(new RecordId("#77:10"));
    Assert.assertTrue(entity.isDirty());

    boolean expectCME = false;
    if (RecordInternal.isContentChanged(entity)) {
      assertEmbedded(true);
      expectCME = true;
    } else {
      assertEmbedded(false);
    }

    entity.save();
    db.commit();


    db.begin();
    var version = entity.getVersion();
    entity = db.bindToSession(entity);
    ridBag = entity.field("ridBag");
    ridBag.add(new RecordId("#77:12"));

    entity.save();
    db.commit();

    db.begin();
    entity = db.bindToSession(entity);
    Assert.assertEquals(version == entity.getVersion(), !expectCME);
    db.rollback();
  }

  public void testAddAllAndIterator() throws Exception {
    final Set<Identifiable> expected = new HashSet<Identifiable>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));

    RidBag bag = new RidBag(db);

    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    assertEquals(bag.size(), 5);

    Set<Identifiable> actual = new HashSet<Identifiable>(8);
    for (Identifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  public void testAddSBTreeAddInMemoryIterate() {
    List<Identifiable> rids = new ArrayList<Identifiable>();

    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);

    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();

    db.close();

    db = createSessionInstance();

    db.begin();
    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:0"));
    rids.add(new RecordId("#77:0"));

    bag.add(new RecordId("#77:1"));
    rids.add(new RecordId("#77:1"));

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:5"));
    rids.add(new RecordId("#77:5"));

    bag.add(new RecordId("#77:6"));
    rids.add(new RecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = ((EntityImpl) db.newEntity());
    final RidBag otherBag = new RidBag(db);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    doc.field("ridbag", otherBag);

    doc.save();
    db.commit();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testCycle() {
    EntityImpl docOne = ((EntityImpl) db.newEntity());
    RidBag ridBagOne = new RidBag(db);

    EntityImpl docTwo = ((EntityImpl) db.newEntity());
    RidBag ridBagTwo = new RidBag(db);

    docOne.field("ridBag", ridBagOne);
    docTwo.field("ridBag", ridBagTwo);

    db.begin();
    docOne.save();
    docTwo.save();
    db.commit();

    db.begin();
    docOne = db.bindToSession(docOne);
    docTwo = db.bindToSession(docTwo);

    ridBagOne = docOne.field("ridBag");
    ridBagOne.add(docTwo);

    ridBagTwo = docTwo.field("ridBag");
    ridBagTwo.add(docOne);

    docOne.save();
    db.commit();

    docOne = db.load(docOne.getIdentity());
    ridBagOne = docOne.field("ridBag");

    docTwo = db.load(docTwo.getIdentity());
    ridBagTwo = docTwo.field("ridBag");

    Assert.assertEquals(ridBagOne.iterator().next(), docTwo);
    Assert.assertEquals(ridBagTwo.iterator().next(), docOne);
  }

  public void testAddSBTreeAddInMemoryIterateAndRemove() {
    List<Identifiable> rids = new ArrayList<Identifiable>();

    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:4"));
    rids.add(new RecordId("#77:4"));

    bag.add(new RecordId("#77:7"));
    rids.add(new RecordId("#77:7"));

    bag.add(new RecordId("#77:8"));
    rids.add(new RecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("ridbag", bag);

    db.begin();
    doc.save();
    db.commit();

    RID rid = doc.getIdentity();
    db.close();

    db = createSessionInstance();

    db.begin();
    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new RecordId("#77:0"));
    rids.add(new RecordId("#77:0"));

    bag.add(new RecordId("#77:1"));
    rids.add(new RecordId("#77:1"));

    bag.add(new RecordId("#77:2"));
    rids.add(new RecordId("#77:2"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:3"));
    rids.add(new RecordId("#77:3"));

    bag.add(new RecordId("#77:5"));
    rids.add(new RecordId("#77:5"));

    bag.add(new RecordId("#77:6"));
    rids.add(new RecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    Iterator<Identifiable> iterator = bag.iterator();
    int r2c = 0;
    int r3c = 0;
    int r6c = 0;
    int r4c = 0;
    int r7c = 0;

    while (iterator.hasNext()) {
      Identifiable identifiable = iterator.next();
      if (identifiable.equals(new RecordId("#77:2"))) {
        if (r2c < 2) {
          r2c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new RecordId("#77:3"))) {
        if (r3c < 1) {
          r3c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new RecordId("#77:6"))) {
        if (r6c < 1) {
          r6c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new RecordId("#77:4"))) {
        if (r4c < 1) {
          r4c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new RecordId("#77:7"))) {
        if (r7c < 1) {
          r7c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }
    }

    assertEquals(r2c, 2);
    assertEquals(r3c, 1);
    assertEquals(r6c, 1);
    assertEquals(r4c, 1);
    assertEquals(r7c, 1);

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (Identifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = ((EntityImpl) db.newEntity());

    final RidBag otherBag = new RidBag(db);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());

    doc.field("ridbag", otherBag);

    doc.save();
    db.commit();

    rid = doc.getIdentity();

    doc = db.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testRemove() {
    final Set<Identifiable> expected = new HashSet<Identifiable>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));

    final RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());
    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    bag.remove(new RecordId("#77:23"));
    assertEmbedded(bag.isEmbedded());

    final Set<Identifiable> expectedTwo = new HashSet<Identifiable>(8);
    expectedTwo.addAll(expected);

    for (Identifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }

    Assert.assertTrue(expectedTwo.isEmpty());

    expected.remove(new RecordId("#77:14"));
    bag.remove(new RecordId("#77:14"));
    assertEmbedded(bag.isEmbedded());

    expectedTwo.addAll(expected);

    for (Identifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }
  }

  public void testSaveLoad() throws Exception {
    Set<Identifiable> expected = new HashSet<Identifiable>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));
    expected.add(new RecordId("#77:17"));
    expected.add(new RecordId("#77:18"));
    expected.add(new RecordId("#77:19"));
    expected.add(new RecordId("#77:20"));
    expected.add(new RecordId("#77:21"));
    expected.add(new RecordId("#77:22"));

    EntityImpl doc = ((EntityImpl) db.newEntity());

    final RidBag bag = new RidBag(db);
    bag.addAll(expected);

    doc.field("ridbag", bag);
    assertEmbedded(bag.isEmbedded());

    db.begin();
    doc.save();
    db.commit();
    final RID id = doc.getIdentity();

    db.close();

    db = createSessionInstance();

    doc = db.load(id);
    doc.setLazyLoad(false);

    final RidBag loaded = doc.field("ridbag");
    assertEmbedded(loaded.isEmbedded());

    Assert.assertEquals(loaded.size(), expected.size());
    for (Identifiable identifiable : loaded) {
      Assert.assertTrue(expected.remove(identifiable));
    }

    Assert.assertTrue(expected.isEmpty());
  }

  public void testSaveInBackOrder() throws Exception {
    EntityImpl docA = ((EntityImpl) db.newEntity()).field("name", "A");

    db.begin();
    EntityImpl docB =
        ((EntityImpl) db.newEntity())
            .field("name", "B");
    docB.save();
    db.commit();

    RidBag ridBag = new RidBag(db);

    ridBag.add(docA);
    ridBag.add(docB);

    db.begin();
    docA.save();
    db.commit();

    ridBag.remove(docB);

    assertEmbedded(ridBag.isEmbedded());

    HashSet<Identifiable> result = new HashSet<Identifiable>();

    for (Identifiable oIdentifiable : ridBag) {
      result.add(oIdentifiable);
    }

    Assert.assertTrue(result.contains(docA));
    Assert.assertFalse(result.contains(docB));
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(ridBag.size(), 1);
  }

  public void testMassiveChanges() {
    EntityImpl document = ((EntityImpl) db.newEntity());
    RidBag bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    final long seed = System.nanoTime();
    System.out.println("testMassiveChanges seed: " + seed);

    Random random = new Random(seed);
    List<Identifiable> rids = new ArrayList<Identifiable>();
    document.field("bag", bag);

    db.begin();
    document.save();
    db.commit();

    RID rid = document.getIdentity();

    for (int i = 0; i < 10; i++) {
      db.begin();
      document = db.load(rid);
      document.setLazyLoad(false);

      bag = document.field("bag");
      assertEmbedded(bag.isEmbedded());

      massiveInsertionIteration(random, rids, bag);
      assertEmbedded(bag.isEmbedded());

      document.save();
      db.commit();
    }

    db.begin();
    db.bindToSession(document).delete();
    db.commit();
  }

  public void testSimultaneousIterationAndRemove() {
    db.begin();
    RidBag ridBag = new RidBag(db);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd);
    }
    document.save();
    db.commit();

    db.begin();
    assertEmbedded(ridBag.isEmbedded());
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    Set<Identifiable> docs = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Identifiable id : ridBag) {
      // cache record inside session
      docs.add(id.getRecord(db));
    }

    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docs.remove(identifiable.getRecord(db)));
      ridBag.remove(identifiable);
      Assert.assertEquals(ridBag.size(), docs.size());

      int counter = 0;
      for (Identifiable id : ridBag) {
        Assert.assertTrue(docs.contains(id.getRecord(db)));
        counter++;
      }

      Assert.assertEquals(counter, docs.size());
      assertEmbedded(ridBag.isEmbedded());
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), 0);
    Assert.assertEquals(docs.size(), 0);
    db.rollback();
  }

  public void testAddMixedValues() {
    db.begin();
    RidBag ridBag = new RidBag(db);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    List<Identifiable> itemsToAdd = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      ridBag = document.field("ridBag");
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
      document.save();
    }
    db.commit();

    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      db.begin();
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();

      document = db.bindToSession(document);
      ridBag = document.field("ridBag");
      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
      document.save();

      db.commit();
    }

    for (int i = 0; i < 10; i++) {
      db.begin();
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      document = db.bindToSession(document);
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);

      document.save();
      db.commit();
    }

    assertEmbedded(ridBag.isEmbedded());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
    }
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    for (Identifiable id : ridBag) {
      Assert.assertTrue(itemsToAdd.remove(id));
    }

    Assert.assertTrue(itemsToAdd.isEmpty());
    db.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBack() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (db.getStorage() instanceof StorageProxy) {
      ServerAdmin server = new ServerAdmin(db.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    db.begin();
    RidBag ridBag = new RidBag(db);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<Identifiable> addedItems = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);
      addedItems.add(docToAdd);
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());
    db.rollback();

    db.begin();
    EntityImpl docToAdd = ((EntityImpl) db.newEntity());
    docToAdd.save();
    db.commit();
    db.begin();

    docToAdd = db.bindToSession(docToAdd);
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    ridBag.add(docToAdd);
    addedItems.add(docToAdd);

    document.save();
    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    List<Identifiable> addedItemsCopy = new ArrayList<>(addedItems);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    addedItems.addAll(addedItemsCopy);

    for (int i = 0; i < 3; i++) {
      ridBag.remove(addedItems.remove(i));
    }

    addedItemsCopy.clear();
    addedItemsCopy.addAll(addedItems);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());
    db.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBackTx() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (db.isRemote()) {
      ServerAdmin server = new ServerAdmin(db.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    RidBag ridBag = new RidBag(db);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());
    db.begin();
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<Identifiable> addedItems = new ArrayList<Identifiable>();

    ridBag = document.field("ridBag");
    for (int i = 0; i < 6; i++) {

      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();
      ridBag.add(docToAdd);
      addedItems.add(docToAdd);
    }
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    EntityImpl docToAdd = ((EntityImpl) db.newEntity());

    docToAdd.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    docToAdd = db.bindToSession(docToAdd);

    ridBag = document.field("ridBag");
    ridBag.add(docToAdd);
    addedItems.add(docToAdd);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    List<Identifiable> addedItemsCopy = new ArrayList<Identifiable>(addedItems);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    addedItems.addAll(addedItemsCopy);

    for (int i = 0; i < 3; i++) {
      ridBag.remove(addedItems.remove(i));
    }

    addedItemsCopy.clear();
    addedItemsCopy.addAll(addedItems);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());
    db.rollback();
  }

  public void testRemoveSavedInCommit() {
    db.begin();
    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    RidBag ridBag = new RidBag(db);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    for (int i = 0; i < 5; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);

      docsToAdd.add(docToAdd);
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    ridBag = document.field("ridBag");
    for (int i = 0; i < 5; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd);

      docsToAdd.add(docToAdd);
    }

    for (int i = 5; i < 10; i++) {
      EntityImpl docToAdd = docsToAdd.get(i).getRecord(db);
      docToAdd.save();
    }

    Iterator<Identifiable> iterator = docsToAdd.listIterator(7);
    while (iterator.hasNext()) {
      Identifiable docToAdd = iterator.next();
      ridBag.remove(docToAdd);
      iterator.remove();
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    List<Identifiable> docsToAddCopy = new ArrayList<Identifiable>(docsToAdd);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(docsToAdd.remove(id));
    }

    Assert.assertTrue(docsToAdd.isEmpty());

    docsToAdd.addAll(docsToAddCopy);

    ridBag = document.field("ridBag");

    for (Identifiable id : ridBag) {
      Assert.assertTrue(docsToAdd.remove(id));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
    db.rollback();
  }

  @Test
  public void testSizeNotChangeAfterRemoveNotExistentElement() {
    final EntityImpl bob = ((EntityImpl) db.newEntity());

    db.begin();
    final EntityImpl fred = ((EntityImpl) db.newEntity());
    fred.save();
    final EntityImpl jim =
        ((EntityImpl) db.newEntity());
    jim.save();
    db.commit();

    RidBag teamMates = new RidBag(db);

    teamMates.add(bob);
    teamMates.add(fred);

    Assert.assertEquals(teamMates.size(), 2);

    teamMates.remove(jim);

    Assert.assertEquals(teamMates.size(), 2);
  }

  @Test
  public void testRemoveNotExistentElementAndAddIt() throws Exception {
    RidBag teamMates = new RidBag(db);

    db.begin();
    final EntityImpl bob = ((EntityImpl) db.newEntity());
    bob.save();
    db.commit();

    teamMates.remove(bob);

    Assert.assertEquals(teamMates.size(), 0);

    teamMates.add(bob);

    Assert.assertEquals(teamMates.size(), 1);
    Assert.assertEquals(teamMates.iterator().next().getIdentity(), bob.getIdentity());
  }

  public void testAddNewItemsAndRemoveThem() {
    db.begin();
    final List<Identifiable> rids = new ArrayList<Identifiable>();
    RidBag ridBag = new RidBag(db);
    int size = 0;
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        size++;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    EntityImpl document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), size);

    final List<Identifiable> newDocs = new ArrayList<Identifiable>();
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();
      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        newDocs.add(docToAdd);
        size++;
      }
    }
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), size);

    Random rnd = new Random();

    for (int i = 0; i < newDocs.size(); i++) {
      if (rnd.nextBoolean()) {
        Identifiable newDoc = newDocs.get(i);
        rids.remove(newDoc);
        ridBag.remove(newDoc);
        newDocs.remove(newDoc);

        size--;
      }
    }

    for (Identifiable identifiable : ridBag) {
      if (newDocs.contains(identifiable) && rnd.nextBoolean()) {
        ridBag.remove(identifiable);
        rids.remove(identifiable);

        size--;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    List<Identifiable> ridsCopy = new ArrayList<Identifiable>(rids);

    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(rids.remove(identifiable));
    }

    Assert.assertTrue(rids.isEmpty());

    document.save();
    db.commit();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    rids.addAll(ridsCopy);
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(rids.remove(identifiable));
    }

    Assert.assertTrue(rids.isEmpty());
    Assert.assertEquals(ridBag.size(), size);
  }

  @Test
  public void testJsonSerialization() {
    db.begin();
    final EntityImpl externalDoc = ((EntityImpl) db.newEntity());

    final RidBag highLevelRidBag = new RidBag(db);

    for (int i = 0; i < 10; i++) {
      var doc = ((EntityImpl) db.newEntity());
      doc.save();

      highLevelRidBag.add(doc);
    }

    externalDoc.save();

    EntityImpl testDocument = ((EntityImpl) db.newEntity());
    testDocument.field("type", "testDocument");
    testDocument.field("ridBag", highLevelRidBag);
    testDocument.field("externalDoc", externalDoc);

    testDocument.save();
    testDocument.save();
    db.commit();

    db.begin();

    testDocument = db.bindToSession(testDocument);
    final String json = testDocument.toJSON(RecordAbstract.OLD_FORMAT_WITH_LATE_TYPES);

    final EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.fromJSON(json);

    Assert.assertTrue(
        EntityHelper.hasSameContentOf(doc, db, testDocument, db, null));
    db.rollback();
  }

  protected abstract void assertEmbedded(boolean isEmbedded);

  private static void massiveInsertionIteration(Random rnd, List<Identifiable> rids,
      RidBag bag) {
    Iterator<Identifiable> bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      Identifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }

    Assert.assertEquals(bag.size(), rids.size());

    for (int i = 0; i < 100; i++) {
      if (rnd.nextDouble() < 0.2 & rids.size() > 5) {
        final int index = rnd.nextInt(rids.size());
        final Identifiable rid = rids.remove(index);
        bag.remove(rid);
      } else {
        final long position;
        position = rnd.nextInt(300);

        final RecordId recordId = new RecordId(1, position);
        rids.add(recordId);
        bag.add(recordId);
      }
    }

    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final Identifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));

      if (rnd.nextDouble() < 0.05) {
        bagIterator.remove();
        Assert.assertTrue(rids.remove(bagValue));
      }
    }

    Assert.assertEquals(bag.size(), rids.size());
    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final Identifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }
  }
}
