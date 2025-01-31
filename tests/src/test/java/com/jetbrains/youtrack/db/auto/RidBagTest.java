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

  public void testAdd() {
    var bag = new RidBag(db);

    bag.add(new RecordId("#77:1"));
    Assert.assertTrue(bag.contains(new RecordId("#77:1")));
    Assert.assertFalse(bag.contains(new RecordId("#78:2")));

    var iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    Identifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new RecordId("#77:1"));

    Assert.assertFalse(iterator.hasNext());
    assertEmbedded(bag.isEmbedded());
  }

  public void testAdd2() {
    var bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));

    Assert.assertTrue(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:3")));

    assertEquals(bag.size(), 2);
    assertEmbedded(bag.isEmbedded());
  }

  public void testAddRemoveInTheMiddleOfIteration() {
    var bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    var counter = 0;
    var iterator = bag.iterator();

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

    var doc = ((EntityImpl) db.newEntity());
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
    var bag = new RidBag(db);

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

    var doc = ((EntityImpl) db.newEntity());
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
    var bag = new RidBag(db);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    var doc = ((EntityImpl) db.newEntity());
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
    var otherBag = new RidBag(db);
    for (var id : bag) {
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
    var bag = new RidBag(db);
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

    var doc = ((EntityImpl) db.newEntity());
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

    var iterator = bag.iterator();
    while (iterator.hasNext()) {
      final var identifiable = iterator.next();
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

    final var otherBag = new RidBag(db);
    for (var id : bag) {
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

  public void testEmptyIterator() {
    var bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());
    assertEquals(bag.size(), 0);

    for (@SuppressWarnings("unused") Identifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    db.begin();
    List<Identifiable> rids = new ArrayList<>();

    var bag = new RidBag(db);
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

    var doc = ((EntityImpl) db.newEntity());
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

    for (var identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testContentChange() {
    var entity = ((EntityImpl) db.newEntity());
    var ridBag = new RidBag(db);
    entity.field("ridBag", ridBag);

    db.begin();
    entity.save();
    db.commit();

    db.begin();
    entity = db.bindToSession(entity);
    ridBag = entity.field("ridBag");
    ridBag.add(new RecordId("#77:10"));
    Assert.assertTrue(entity.isDirty());

    var expectCME = false;
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

  public void testAddAllAndIterator() {
    final Set<RID> expected = new HashSet<>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));

    var bag = new RidBag(db);

    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    assertEquals(bag.size(), 5);

    Set<Identifiable> actual = new HashSet<>(8);
    for (Identifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  public void testAddSBTreeAddInMemoryIterate() {
    List<Identifiable> rids = new ArrayList<>();

    var bag = new RidBag(db);
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

    var doc = ((EntityImpl) db.newEntity());
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
    final var otherBag = new RidBag(db);
    for (var id : bag) {
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
    var docOne = ((EntityImpl) db.newEntity());
    var ridBagOne = new RidBag(db);

    var docTwo = ((EntityImpl) db.newEntity());
    var ridBagTwo = new RidBag(db);

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
    ridBagOne.add(docTwo.getIdentity());

    ridBagTwo = docTwo.field("ridBag");
    ridBagTwo.add(docOne.getIdentity());

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
    List<Identifiable> rids = new ArrayList<>();

    var bag = new RidBag(db);
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

    var doc = ((EntityImpl) db.newEntity());
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

    var iterator = bag.iterator();
    var r2c = 0;
    var r3c = 0;
    var r6c = 0;
    var r4c = 0;
    var r7c = 0;

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

    final var otherBag = new RidBag(db);
    for (var id : bag) {
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
    final Set<RID> expected = new HashSet<>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));

    final var bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());
    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    bag.remove(new RecordId("#77:23"));
    assertEmbedded(bag.isEmbedded());

    final Set<Identifiable> expectedTwo = new HashSet<>(8);
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

  public void testSaveLoad() {
    Set<RID> expected = new HashSet<>(8);

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

    var doc = ((EntityImpl) db.newEntity());

    final var bag = new RidBag(db);
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
    for (var identifiable : loaded) {
      Assert.assertTrue(expected.remove(identifiable));
    }

    Assert.assertTrue(expected.isEmpty());
  }

  public void testSaveInBackOrder() {
    var docA = ((EntityImpl) db.newEntity()).field("name", "A");

    db.begin();
    var docB =
        ((EntityImpl) db.newEntity())
            .field("name", "B");
    docB.save();
    db.commit();

    var ridBag = new RidBag(db);

    ridBag.add(docA.getIdentity());
    ridBag.add(docB.getIdentity());

    db.begin();
    docA.save();
    db.commit();

    ridBag.remove(docB.getIdentity());

    assertEmbedded(ridBag.isEmbedded());

    var result = new HashSet<Identifiable>();

    for (Identifiable oIdentifiable : ridBag) {
      result.add(oIdentifiable);
    }

    Assert.assertTrue(result.contains(docA));
    Assert.assertFalse(result.contains(docB));
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(ridBag.size(), 1);
  }

  public void testMassiveChanges() {
    var document = ((EntityImpl) db.newEntity());
    var bag = new RidBag(db);
    assertEmbedded(bag.isEmbedded());

    final var seed = System.nanoTime();
    System.out.println("testMassiveChanges seed: " + seed);

    var random = new Random(seed);
    List<Identifiable> rids = new ArrayList<>();
    document.field("bag", bag);

    db.begin();
    document.save();
    db.commit();

    RID rid = document.getIdentity();

    for (var i = 0; i < 10; i++) {
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
    var ridBag = new RidBag(db);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd.getIdentity());
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

    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd.getIdentity());
    }

    assertEmbedded(ridBag.isEmbedded());

    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd.getIdentity());
    }

    assertEmbedded(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docs.remove(identifiable.getRecord(db)));
      ridBag.remove(identifiable.getIdentity());
      Assert.assertEquals(ridBag.size(), docs.size());

      var counter = 0;
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
    var ridBag = new RidBag(db);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    List<Identifiable> itemsToAdd = new ArrayList<>();

    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      ridBag = document.field("ridBag");
      docToAdd.save();

      for (var k = 0; k < 2; k++) {
        ridBag.add(docToAdd.getIdentity());
        itemsToAdd.add(docToAdd);
      }
      document.save();
    }
    db.commit();

    assertEmbedded(ridBag.isEmbedded());

    for (var i = 0; i < 10; i++) {
      db.begin();
      var docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();

      document = db.bindToSession(document);
      ridBag = document.field("ridBag");
      for (var k = 0; k < 2; k++) {
        ridBag.add(docToAdd.getIdentity());
        itemsToAdd.add(docToAdd);
      }
      document.save();

      db.commit();
    }

    for (var i = 0; i < 10; i++) {
      db.begin();
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      document = db.bindToSession(document);
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd.getIdentity());
      itemsToAdd.add(docToAdd);

      document.save();
      db.commit();
    }

    assertEmbedded(ridBag.isEmbedded());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      for (var k = 0; k < 2; k++) {
        ridBag.add(docToAdd.getIdentity());
        itemsToAdd.add(docToAdd);
      }
    }
    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd.getIdentity());
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
      var server = new ServerAdmin(db.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    db.begin();
    var ridBag = new RidBag(db);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<RID> addedItems = new ArrayList<>();
    for (var i = 0; i < 6; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());

      db.begin();
      docToAdd.save();
      db.commit();

      ridBag = document.field("ridBag");
      ridBag.add(docToAdd.getIdentity());
      addedItems.add(docToAdd.getIdentity());
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());
    db.rollback();

    db.begin();
    var docToAdd = ((EntityImpl) db.newEntity());
    docToAdd.save();
    db.commit();
    db.begin();

    docToAdd = db.bindToSession(docToAdd);
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    ridBag.add(docToAdd.getIdentity());
    addedItems.add(docToAdd.getIdentity());

    document.save();
    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    List<RID> addedItemsCopy = new ArrayList<>(addedItems);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id.getIdentity()));
    }

    Assert.assertTrue(addedItems.isEmpty());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id.getIdentity()));
    }

    Assert.assertTrue(addedItems.isEmpty());

    addedItems.addAll(addedItemsCopy);

    for (var i = 0; i < 3; i++) {
      ridBag.remove(addedItems.remove(i).getIdentity());
    }

    addedItemsCopy.clear();
    addedItemsCopy.addAll(addedItems);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    for (var id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (var id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());
    db.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBackTx() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (db.isRemote()) {
      var server = new ServerAdmin(db.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    var ridBag = new RidBag(db);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());
    db.begin();
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<Identifiable> addedItems = new ArrayList<>();

    ridBag = document.field("ridBag");
    for (var i = 0; i < 6; i++) {

      var docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();
      ridBag.add(docToAdd.getIdentity());
      addedItems.add(docToAdd);
    }
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    var docToAdd = ((EntityImpl) db.newEntity());

    docToAdd.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    docToAdd = db.bindToSession(docToAdd);

    ridBag = document.field("ridBag");
    ridBag.add(docToAdd.getIdentity());
    addedItems.add(docToAdd);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    List<Identifiable> addedItemsCopy = new ArrayList<>(addedItems);
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

    for (var i = 0; i < 3; i++) {
      ridBag.remove(addedItems.remove(i).getIdentity());
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
    List<Identifiable> docsToAdd = new ArrayList<>();

    var ridBag = new RidBag(db);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);

    for (var i = 0; i < 5; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd.getIdentity());

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
    for (var i = 0; i < 5; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();
      ridBag.add(docToAdd.getIdentity());

      docsToAdd.add(docToAdd);
    }

    for (var i = 5; i < 10; i++) {
      EntityImpl docToAdd = docsToAdd.get(i).getRecord(db);
      docToAdd.save();
    }

    Iterator<Identifiable> iterator = docsToAdd.listIterator(7);
    while (iterator.hasNext()) {
      var docToAdd = iterator.next();
      ridBag.remove(docToAdd.getIdentity());
      iterator.remove();
    }

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    List<Identifiable> docsToAddCopy = new ArrayList<>(docsToAdd);
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
    final var bob = ((EntityImpl) db.newEntity());

    db.begin();
    final var fred = ((EntityImpl) db.newEntity());
    fred.save();
    final var jim =
        ((EntityImpl) db.newEntity());
    jim.save();
    db.commit();

    var teamMates = new RidBag(db);

    teamMates.add(bob.getIdentity());
    teamMates.add(fred.getIdentity());

    Assert.assertEquals(teamMates.size(), 2);

    teamMates.remove(jim.getIdentity());

    Assert.assertEquals(teamMates.size(), 2);
  }

  @Test
  public void testRemoveNotExistentElementAndAddIt() {
    var teamMates = new RidBag(db);

    db.begin();
    final var bob = ((EntityImpl) db.newEntity());
    bob.save();
    db.commit();

    teamMates.remove(bob.getIdentity());

    Assert.assertEquals(teamMates.size(), 0);

    teamMates.add(bob.getIdentity());

    Assert.assertEquals(teamMates.size(), 1);
    Assert.assertEquals(teamMates.iterator().next().getIdentity(), bob.getIdentity());
  }

  public void testAddNewItemsAndRemoveThem() {
    db.begin();
    final List<Identifiable> rids = new ArrayList<>();
    var ridBag = new RidBag(db);
    var size = 0;
    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());
      docToAdd.save();

      for (var k = 0; k < 2; k++) {
        ridBag.add(docToAdd.getIdentity());
        rids.add(docToAdd);
        size++;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    var document = ((EntityImpl) db.newEntity());
    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), size);

    final List<Identifiable> newDocs = new ArrayList<>();
    for (var i = 0; i < 10; i++) {
      var docToAdd = ((EntityImpl) db.newEntity());

      docToAdd.save();
      for (var k = 0; k < 2; k++) {
        ridBag.add(docToAdd.getIdentity());
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

    var rnd = new Random();

    for (var i = 0; i < newDocs.size(); i++) {
      if (rnd.nextBoolean()) {
        var newDoc = newDocs.get(i);
        rids.remove(newDoc);
        ridBag.remove(newDoc.getIdentity());
        newDocs.remove(newDoc);

        size--;
      }
    }

    for (Identifiable identifiable : ridBag) {
      if (newDocs.contains(identifiable) && rnd.nextBoolean()) {
        ridBag.remove(identifiable.getIdentity());
        rids.remove(identifiable);

        size--;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    List<Identifiable> ridsCopy = new ArrayList<>(rids);

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
    final var externalDoc = ((EntityImpl) db.newEntity());

    final var highLevelRidBag = new RidBag(db);

    for (var i = 0; i < 10; i++) {
      var doc = ((EntityImpl) db.newEntity());
      doc.save();

      highLevelRidBag.add(doc.getIdentity());
    }

    externalDoc.save();

    var testDocument = ((EntityImpl) db.newEntity());
    testDocument.field("type", "testDocument");
    testDocument.field("ridBag", highLevelRidBag);
    testDocument.field("externalDoc", externalDoc);

    testDocument.save();
    testDocument.save();
    db.commit();

    db.begin();

    testDocument = db.bindToSession(testDocument);
    final var json = testDocument.toJSON(RecordAbstract.OLD_FORMAT_WITH_LATE_TYPES);

    final var doc = ((EntityImpl) db.newEntity());
    doc.updateFromJSON(json);

    Assert.assertTrue(
        EntityHelper.hasSameContentOf(doc, db, testDocument, db, null));
    db.rollback();
  }

  protected abstract void assertEmbedded(boolean isEmbedded);

  private static void massiveInsertionIteration(Random rnd, List<Identifiable> rids,
      RidBag bag) {
    var bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      Identifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }

    Assert.assertEquals(bag.size(), rids.size());

    for (var i = 0; i < 100; i++) {
      if (rnd.nextDouble() < 0.2 & rids.size() > 5) {
        final var index = rnd.nextInt(rids.size());
        final var rid = rids.remove(index);
        bag.remove(rid.getIdentity());
      } else {
        final long position;
        position = rnd.nextInt(300);

        final var recordId = new RecordId(1, position);
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
