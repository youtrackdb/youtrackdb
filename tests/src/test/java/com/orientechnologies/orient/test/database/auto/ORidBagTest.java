package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentHelper;
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
public abstract class ORidBagTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public ORidBagTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testAdd() throws Exception {
    RidBag bag = new RidBag(database);

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
    RidBag bag = new RidBag(database);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));

    Assert.assertTrue(bag.contains(new RecordId("#77:2")));
    Assert.assertFalse(bag.contains(new RecordId("#77:3")));

    assertEquals(bag.size(), 2);
    assertEmbedded(bag.isEmbedded());
  }

  public void testAddRemoveInTheMiddleOfIteration() {
    RidBag bag = new RidBag(database);

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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();

    doc = database.load(rid);
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
    RidBag bag = new RidBag(database);

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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();

    doc = database.load(rid);
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
    RidBag bag = new RidBag(database);

    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:2"));
    bag.add(new RecordId("#77:3"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:4"));
    bag.add(new RecordId("#77:5"));
    bag.add(new RecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    doc = database.load(rid);
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

    doc = new EntityImpl();
    RidBag otherBag = new RidBag(database);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    rid = doc.getIdentity();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveDuringIterationSBTreeContainsValues() {
    database.begin();
    RidBag bag = new RidBag(database);
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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    doc = database.load(rid);
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
    doc = new EntityImpl();

    final RidBag otherBag = new RidBag(database);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());
    doc.field("ridbag", otherBag);


    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    rid = doc.getIdentity();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testEmptyIterator() throws Exception {
    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());
    assertEquals(bag.size(), 0);

    for (Identifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    List<Identifiable> rids = new ArrayList<Identifiable>();

    RidBag bag = new RidBag(database);
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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
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
    database.commit();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testContentChange() {
    EntityImpl document = new EntityImpl();
    RidBag ridBag = new RidBag(database);
    document.field("ridBag", ridBag);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    ridBag.add(new RecordId("#77:10"));
    Assert.assertTrue(document.isDirty());

    boolean expectCME = false;
    if (RecordInternal.isContentChanged(document)) {
      assertEmbedded(true);
      expectCME = true;
    } else {
      assertEmbedded(false);
    }


    document.save();
    database.commit();

    database.begin();
    EntityImpl copy = new EntityImpl();
    RecordInternal.unsetDirty(copy);
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    copy.fromStream(document.toStream());
    RecordInternal.setIdentity(copy, new RecordId(document.getIdentity()));
    RecordInternal.setVersion(copy, document.getVersion());

    RidBag copyRidBag = copy.field("ridBag");
    Assert.assertNotSame(copyRidBag, ridBag);

    copyRidBag.add(new RecordId("#77:11"));
    Assert.assertTrue(copy.isDirty());
    Assert.assertFalse(document.isDirty());

    ridBag.add(new RecordId("#77:12"));
    Assert.assertTrue(document.isDirty());

    document.save();
    database.commit();
    try {
      database.begin();
      copy.save();
      database.commit();
      Assert.assertFalse(expectCME);
    } catch (ConcurrentModificationException cme) {
      Assert.assertTrue(expectCME);
    }
  }

  public void testAddAllAndIterator() throws Exception {
    final Set<Identifiable> expected = new HashSet<Identifiable>(8);

    expected.add(new RecordId("#77:12"));
    expected.add(new RecordId("#77:13"));
    expected.add(new RecordId("#77:14"));
    expected.add(new RecordId("#77:15"));
    expected.add(new RecordId("#77:16"));

    RidBag bag = new RidBag(database);

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

    RidBag bag = new RidBag(database);
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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
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

    doc = new EntityImpl();
    final RidBag otherBag = new RidBag(database);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    doc.field("ridbag", otherBag);

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    rid = doc.getIdentity();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (Identifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testCycle() {
    EntityImpl docOne = new EntityImpl();
    RidBag ridBagOne = new RidBag(database);

    EntityImpl docTwo = new EntityImpl();
    RidBag ridBagTwo = new RidBag(database);

    docOne.field("ridBag", ridBagOne);
    docTwo.field("ridBag", ridBagTwo);

    database.begin();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    docOne = database.bindToSession(docOne);
    docTwo = database.bindToSession(docTwo);

    ridBagOne = docOne.field("ridBag");
    ridBagOne.add(docTwo);

    ridBagTwo = docTwo.field("ridBag");
    ridBagTwo.add(docOne);

    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    docOne = database.load(docOne.getIdentity());
    ridBagOne = docOne.field("ridBag");

    docTwo = database.load(docTwo.getIdentity());
    ridBagTwo = docTwo.field("ridBag");

    Assert.assertEquals(ridBagOne.iterator().next(), docTwo);
    Assert.assertEquals(ridBagTwo.iterator().next(), docOne);
  }

  public void testAddSBTreeAddInMemoryIterateAndRemove() {
    List<Identifiable> rids = new ArrayList<Identifiable>();

    RidBag bag = new RidBag(database);
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

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = doc.getIdentity();
    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
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

    doc = new EntityImpl();

    final RidBag otherBag = new RidBag(database);
    for (Identifiable id : bag) {
      otherBag.add(id);
    }

    assertEmbedded(otherBag.isEmbedded());

    doc.field("ridbag", otherBag);

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    rid = doc.getIdentity();

    doc = database.load(rid);
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

    final RidBag bag = new RidBag(database);
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

    EntityImpl doc = new EntityImpl();

    final RidBag bag = new RidBag(database);
    bag.addAll(expected);

    doc.field("ridbag", bag);
    assertEmbedded(bag.isEmbedded());

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    final RID id = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    doc = database.load(id);
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
    EntityImpl docA = new EntityImpl().field("name", "A");

    database.begin();
    EntityImpl docB =
        new EntityImpl()
            .field("name", "B");
    docB.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RidBag ridBag = new RidBag(database);

    ridBag.add(docA);
    ridBag.add(docB);

    database.begin();
    docA.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
    EntityImpl document = new EntityImpl();
    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());

    final long seed = System.nanoTime();
    System.out.println("testMassiveChanges seed: " + seed);

    Random random = new Random(seed);
    List<Identifiable> rids = new ArrayList<Identifiable>();
    document.field("bag", bag);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RID rid = document.getIdentity();

    for (int i = 0; i < 10; i++) {
      database.begin();
      document = database.load(rid);
      document.setLazyLoad(false);

      bag = document.field("bag");
      assertEmbedded(bag.isEmbedded());

      massiveInsertionIteration(random, rids, bag);
      assertEmbedded(bag.isEmbedded());

      document.save();
      database.commit();
    }

    database.begin();
    database.bindToSession(document).delete();
    database.commit();
  }

  public void testSimultaneousIterationAndRemove() {
    database.begin();
    RidBag ridBag = new RidBag(database);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save();
      ridBag.add(docToAdd);
    }
    document.save();
    database.commit();

    database.begin();
    assertEmbedded(ridBag.isEmbedded());
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");

    Set<Identifiable> docs = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Identifiable id : ridBag) {
      // cache record inside session
      docs.add(id.getRecord());
    }

    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();

      database.begin();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();

      database.begin();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      docs.add(docToAdd);
      ridBag.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docs.remove(identifiable.getRecord()));
      ridBag.remove(identifiable);
      Assert.assertEquals(ridBag.size(), docs.size());

      int counter = 0;
      for (Identifiable id : ridBag) {
        Assert.assertTrue(docs.contains(id.getRecord()));
        counter++;
      }

      Assert.assertEquals(counter, docs.size());
      assertEmbedded(ridBag.isEmbedded());
    }

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), 0);
    Assert.assertEquals(docs.size(), 0);
    database.rollback();
  }

  public void testAddMixedValues() {
    database.begin();
    RidBag ridBag = new RidBag(database);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);
    assertEmbedded(ridBag.isEmbedded());

    List<Identifiable> itemsToAdd = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      ridBag = document.field("ridBag");
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
      document.save();
    }
    database.commit();

    assertEmbedded(ridBag.isEmbedded());

    for (int i = 0; i < 10; i++) {
      database.begin();
      EntityImpl docToAdd = new EntityImpl();

      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));

      document = database.bindToSession(document);
      ridBag = document.field("ridBag");
      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
      document.save();

      database.commit();
    }

    for (int i = 0; i < 10; i++) {
      database.begin();
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));

      document = database.bindToSession(document);
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);

      document.save();
      database.commit();
    }

    assertEmbedded(ridBag.isEmbedded());

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        itemsToAdd.add(docToAdd);
      }
    }
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      ridBag.add(docToAdd);
      itemsToAdd.add(docToAdd);
    }

    assertEmbedded(ridBag.isEmbedded());
    document.save();

    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    Assert.assertEquals(ridBag.size(), itemsToAdd.size());

    for (Identifiable id : ridBag) {
      Assert.assertTrue(itemsToAdd.remove(id));
    }

    Assert.assertTrue(itemsToAdd.isEmpty());
    database.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBack() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (database.getStorage() instanceof StorageProxy) {
      ServerAdmin server = new ServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    database.begin();
    RidBag ridBag = new RidBag(database);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());

    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<Identifiable> addedItems = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      EntityImpl docToAdd = new EntityImpl();

      database.begin();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);
      addedItems.add(docToAdd);
    }

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());
    database.rollback();

    database.begin();
    EntityImpl docToAdd = new EntityImpl();
    docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    database.begin();

    docToAdd = database.bindToSession(docToAdd);
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    ridBag.add(docToAdd);
    addedItems.add(docToAdd);

    document.save();
    database.commit();

    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    List<Identifiable> addedItemsCopy = new ArrayList<>(addedItems);
    for (Identifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    database.begin();
    document = database.bindToSession(document);
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
    database.commit();

    database.begin();
    document = database.bindToSession(document);
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
    database.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBackTx() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (database.isRemote()) {
      ServerAdmin server = new ServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, 7);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    RidBag ridBag = new RidBag(database);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);

    Assert.assertTrue(ridBag.isEmbedded());
    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    List<Identifiable> addedItems = new ArrayList<Identifiable>();

    ridBag = document.field("ridBag");
    for (int i = 0; i < 6; i++) {

      EntityImpl docToAdd = new EntityImpl();

      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      ridBag.add(docToAdd);
      addedItems.add(docToAdd);
    }
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertTrue(ridBag.isEmbedded());

    EntityImpl docToAdd = new EntityImpl();

    docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    docToAdd = database.bindToSession(docToAdd);

    ridBag = document.field("ridBag");
    ridBag.add(docToAdd);
    addedItems.add(docToAdd);

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
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
    database.commit();

    database.begin();
    document = database.bindToSession(document);
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
    database.rollback();
  }

  public void testRemoveSavedInCommit() {
    database.begin();
    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    RidBag ridBag = new RidBag(database);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);

    for (int i = 0; i < 5; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      ridBag = document.field("ridBag");
      ridBag.add(docToAdd);

      docsToAdd.add(docToAdd);
    }

    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    ridBag = document.field("ridBag");
    for (int i = 0; i < 5; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      ridBag.add(docToAdd);

      docsToAdd.add(docToAdd);
    }

    for (int i = 5; i < 10; i++) {
      EntityImpl docToAdd = docsToAdd.get(i).getRecord();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
    }

    Iterator<Identifiable> iterator = docsToAdd.listIterator(7);
    while (iterator.hasNext()) {
      Identifiable docToAdd = iterator.next();
      ridBag.remove(docToAdd);
      iterator.remove();
    }

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
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
    database.rollback();
  }

  @Test
  public void testSizeNotChangeAfterRemoveNotExistentElement() {
    final EntityImpl bob = new EntityImpl();

    database.begin();
    final EntityImpl fred = new EntityImpl();
    fred.save(database.getClusterNameById(database.getDefaultClusterId()));
    final EntityImpl jim =
        new EntityImpl();
    jim.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    RidBag teamMates = new RidBag(database);

    teamMates.add(bob);
    teamMates.add(fred);

    Assert.assertEquals(teamMates.size(), 2);

    teamMates.remove(jim);

    Assert.assertEquals(teamMates.size(), 2);
  }

  @Test
  public void testRemoveNotExistentElementAndAddIt() throws Exception {
    RidBag teamMates = new RidBag(database);

    database.begin();
    final EntityImpl bob = new EntityImpl();
    bob.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    teamMates.remove(bob);

    Assert.assertEquals(teamMates.size(), 0);

    teamMates.add(bob);

    Assert.assertEquals(teamMates.size(), 1);
    Assert.assertEquals(teamMates.iterator().next().getIdentity(), bob.getIdentity());
  }

  public void testAddNewItemsAndRemoveThem() {
    database.begin();
    final List<Identifiable> rids = new ArrayList<Identifiable>();
    RidBag ridBag = new RidBag(database);
    int size = 0;
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));

      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        size++;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    EntityImpl document = new EntityImpl();
    document.field("ridBag", ridBag);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.load(document.getIdentity());
    ridBag = document.field("ridBag");
    Assert.assertEquals(ridBag.size(), size);

    final List<Identifiable> newDocs = new ArrayList<Identifiable>();
    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();

      docToAdd.save(database.getClusterNameById(database.getDefaultClusterId()));
      for (int k = 0; k < 2; k++) {
        ridBag.add(docToAdd);
        rids.add(docToAdd);
        newDocs.add(docToAdd);
        size++;
      }
    }
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
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
    database.commit();

    document = database.load(document.getIdentity());
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
    database.begin();
    final EntityImpl externalDoc = new EntityImpl();

    final RidBag highLevelRidBag = new RidBag(database);

    for (int i = 0; i < 10; i++) {
      var doc = new EntityImpl();
      doc.save(database.getClusterNameById(database.getDefaultClusterId()));

      highLevelRidBag.add(doc);
    }

    externalDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl testDocument = new EntityImpl();
    testDocument.field("type", "testDocument");
    testDocument.field("ridBag", highLevelRidBag);
    testDocument.field("externalDoc", externalDoc);

    testDocument.save(database.getClusterNameById(database.getDefaultClusterId()));
    testDocument.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();

    testDocument = database.bindToSession(testDocument);
    final String json = testDocument.toJSON(RecordAbstract.OLD_FORMAT_WITH_LATE_TYPES);

    final EntityImpl doc = new EntityImpl();
    doc.fromJSON(json);

    Assert.assertTrue(
        DocumentHelper.hasSameContentOf(doc, database, testDocument, database, null));
    database.rollback();
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
