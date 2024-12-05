package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentHelper;
import com.jetbrains.youtrack.db.internal.core.storage.OStorageProxy;
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

    bag.add(new YTRecordId("#77:1"));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:1")));
    Assert.assertFalse(bag.contains(new YTRecordId("#78:2")));

    Iterator<YTIdentifiable> iterator = bag.iterator();
    Assert.assertTrue(iterator.hasNext());

    YTIdentifiable identifiable = iterator.next();
    Assert.assertEquals(identifiable, new YTRecordId("#77:1"));

    Assert.assertFalse(iterator.hasNext());
    assertEmbedded(bag.isEmbedded());
  }

  public void testAdd2() throws Exception {
    RidBag bag = new RidBag(database);

    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:2"));

    Assert.assertTrue(bag.contains(new YTRecordId("#77:2")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:3")));

    assertEquals(bag.size(), 2);
    assertEmbedded(bag.isEmbedded());
  }

  public void testAddRemoveInTheMiddleOfIteration() {
    RidBag bag = new RidBag(database);

    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:3"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:5"));
    bag.add(new YTRecordId("#77:6"));

    int counter = 0;
    Iterator<YTIdentifiable> iterator = bag.iterator();

    bag.remove(new YTRecordId("#77:2"));
    while (iterator.hasNext()) {
      counter++;
      if (counter == 1) {
        bag.remove(new YTRecordId("#77:1"));
        bag.remove(new YTRecordId("#77:2"));
      }

      if (counter == 3) {
        bag.remove(new YTRecordId("#77:4"));
      }

      if (counter == 5) {
        bag.remove(new YTRecordId("#77:6"));
      }

      iterator.next();
    }

    Assert.assertTrue(bag.contains(new YTRecordId("#77:3")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:4")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:5")));

    Assert.assertFalse(bag.contains(new YTRecordId("#77:2")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:6")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:1")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:0")));

    assertEmbedded(bag.isEmbedded());

    final List<YTIdentifiable> rids = new ArrayList<>();
    rids.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:5"));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    Assert.assertTrue(bag.contains(new YTRecordId("#77:3")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:4")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:5")));

    Assert.assertFalse(bag.contains(new YTRecordId("#77:2")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:6")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:1")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:0")));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemove() {
    RidBag bag = new RidBag(database);

    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:3"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:5"));
    bag.add(new YTRecordId("#77:6"));

    bag.remove(new YTRecordId("#77:1"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:4"));
    bag.remove(new YTRecordId("#77:6"));

    Assert.assertTrue(bag.contains(new YTRecordId("#77:3")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:4")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:5")));

    Assert.assertFalse(bag.contains(new YTRecordId("#77:2")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:6")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:1")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:0")));

    assertEmbedded(bag.isEmbedded());

    final List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();
    rids.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:5"));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    Assert.assertTrue(bag.contains(new YTRecordId("#77:3")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:4")));
    Assert.assertTrue(bag.contains(new YTRecordId("#77:5")));

    Assert.assertFalse(bag.contains(new YTRecordId("#77:2")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:6")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:1")));
    Assert.assertFalse(bag.contains(new YTRecordId("#77:0")));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveSBTreeContainsValues() {
    RidBag bag = new RidBag(database);

    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:3"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:5"));
    bag.add(new YTRecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new YTRecordId("#77:1"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:4"));
    bag.remove(new YTRecordId("#77:6"));

    final List<YTIdentifiable> rids = new ArrayList<>();
    rids.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:5"));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = new EntityImpl();
    RidBag otherBag = new RidBag(database);
    for (YTIdentifiable id : bag) {
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

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testAddRemoveDuringIterationSBTreeContainsValues() {
    database.begin();
    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:2"));
    bag.add(new YTRecordId("#77:3"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:4"));
    bag.add(new YTRecordId("#77:5"));
    bag.add(new YTRecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.remove(new YTRecordId("#77:1"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:2"));
    bag.remove(new YTRecordId("#77:4"));
    bag.remove(new YTRecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    final List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();
    rids.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:5"));

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    Iterator<YTIdentifiable> iterator = bag.iterator();
    while (iterator.hasNext()) {
      final YTIdentifiable identifiable = iterator.next();
      if (identifiable.equals(new YTRecordId("#77:4"))) {
        iterator.remove();
        assertTrue(rids.remove(identifiable));
      }
    }

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    assertEmbedded(bag.isEmbedded());
    doc = new EntityImpl();

    final RidBag otherBag = new RidBag(database);
    for (YTIdentifiable id : bag) {
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

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testEmptyIterator() throws Exception {
    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());
    assertEquals(bag.size(), 0);

    for (YTIdentifiable id : bag) {
      Assert.fail();
    }
  }

  public void testAddRemoveNotExisting() {
    List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();

    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:5"));
    rids.add(new YTRecordId("#77:5"));

    bag.add(new YTRecordId("#77:6"));
    rids.add(new YTRecordId("#77:6"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.remove(new YTRecordId("#77:4"));
    rids.remove(new YTRecordId("#77:4"));

    bag.remove(new YTRecordId("#77:4"));
    rids.remove(new YTRecordId("#77:4"));

    bag.remove(new YTRecordId("#77:2"));
    rids.remove(new YTRecordId("#77:2"));

    bag.remove(new YTRecordId("#77:2"));
    rids.remove(new YTRecordId("#77:2"));

    bag.remove(new YTRecordId("#77:7"));
    rids.remove(new YTRecordId("#77:7"));

    bag.remove(new YTRecordId("#77:8"));
    rids.remove(new YTRecordId("#77:8"));

    bag.remove(new YTRecordId("#77:8"));
    rids.remove(new YTRecordId("#77:8"));

    bag.remove(new YTRecordId("#77:8"));
    rids.remove(new YTRecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }


    doc.save();
    database.commit();

    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    for (YTIdentifiable identifiable : bag) {
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
    ridBag.add(new YTRecordId("#77:10"));
    Assert.assertTrue(document.isDirty());

    boolean expectCME = false;
    if (ORecordInternal.isContentChanged(document)) {
      assertEmbedded(true);
      expectCME = true;
    } else {
      assertEmbedded(false);
    }


    document.save();
    database.commit();

    database.begin();
    EntityImpl copy = new EntityImpl();
    ORecordInternal.unsetDirty(copy);
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    copy.fromStream(document.toStream());
    ORecordInternal.setIdentity(copy, new YTRecordId(document.getIdentity()));
    ORecordInternal.setVersion(copy, document.getVersion());

    RidBag copyRidBag = copy.field("ridBag");
    Assert.assertNotSame(copyRidBag, ridBag);

    copyRidBag.add(new YTRecordId("#77:11"));
    Assert.assertTrue(copy.isDirty());
    Assert.assertFalse(document.isDirty());

    ridBag.add(new YTRecordId("#77:12"));
    Assert.assertTrue(document.isDirty());

    document.save();
    database.commit();
    try {
      database.begin();
      copy.save();
      database.commit();
      Assert.assertFalse(expectCME);
    } catch (YTConcurrentModificationException cme) {
      Assert.assertTrue(expectCME);
    }
  }

  public void testAddAllAndIterator() throws Exception {
    final Set<YTIdentifiable> expected = new HashSet<YTIdentifiable>(8);

    expected.add(new YTRecordId("#77:12"));
    expected.add(new YTRecordId("#77:13"));
    expected.add(new YTRecordId("#77:14"));
    expected.add(new YTRecordId("#77:15"));
    expected.add(new YTRecordId("#77:16"));

    RidBag bag = new RidBag(database);

    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    assertEquals(bag.size(), 5);

    Set<YTIdentifiable> actual = new HashSet<YTIdentifiable>(8);
    for (YTIdentifiable id : bag) {
      actual.add(id);
    }

    assertEquals(actual, expected);
  }

  public void testAddSBTreeAddInMemoryIterate() {
    List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();

    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));
    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:0"));
    rids.add(new YTRecordId("#77:0"));

    bag.add(new YTRecordId("#77:1"));
    rids.add(new YTRecordId("#77:1"));

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:5"));
    rids.add(new YTRecordId("#77:5"));

    bag.add(new YTRecordId("#77:6"));
    rids.add(new YTRecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = new EntityImpl();
    final RidBag otherBag = new RidBag(database);
    for (YTIdentifiable id : bag) {
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

    for (YTIdentifiable identifiable : bag) {
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
    List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();

    RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:4"));
    rids.add(new YTRecordId("#77:4"));

    bag.add(new YTRecordId("#77:7"));
    rids.add(new YTRecordId("#77:7"));

    bag.add(new YTRecordId("#77:8"));
    rids.add(new YTRecordId("#77:8"));

    assertEmbedded(bag.isEmbedded());

    EntityImpl doc = new EntityImpl();
    doc.field("ridbag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = doc.getIdentity();
    database.close();

    database = createSessionInstance();

    database.begin();
    doc = database.load(rid);
    doc.setLazyLoad(false);

    bag = doc.field("ridbag");
    assertEmbedded(bag.isEmbedded());

    bag.add(new YTRecordId("#77:0"));
    rids.add(new YTRecordId("#77:0"));

    bag.add(new YTRecordId("#77:1"));
    rids.add(new YTRecordId("#77:1"));

    bag.add(new YTRecordId("#77:2"));
    rids.add(new YTRecordId("#77:2"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:3"));
    rids.add(new YTRecordId("#77:3"));

    bag.add(new YTRecordId("#77:5"));
    rids.add(new YTRecordId("#77:5"));

    bag.add(new YTRecordId("#77:6"));
    rids.add(new YTRecordId("#77:6"));

    assertEmbedded(bag.isEmbedded());

    Iterator<YTIdentifiable> iterator = bag.iterator();
    int r2c = 0;
    int r3c = 0;
    int r6c = 0;
    int r4c = 0;
    int r7c = 0;

    while (iterator.hasNext()) {
      YTIdentifiable identifiable = iterator.next();
      if (identifiable.equals(new YTRecordId("#77:2"))) {
        if (r2c < 2) {
          r2c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new YTRecordId("#77:3"))) {
        if (r3c < 1) {
          r3c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new YTRecordId("#77:6"))) {
        if (r6c < 1) {
          r6c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new YTRecordId("#77:4"))) {
        if (r4c < 1) {
          r4c++;
          iterator.remove();
          rids.remove(identifiable);
        }
      }

      if (identifiable.equals(new YTRecordId("#77:7"))) {
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

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());

    for (YTIdentifiable identifiable : bag) {
      rids.add(identifiable);
    }

    doc = new EntityImpl();

    final RidBag otherBag = new RidBag(database);
    for (YTIdentifiable id : bag) {
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

    for (YTIdentifiable identifiable : bag) {
      assertTrue(rids.remove(identifiable));
    }

    assertTrue(rids.isEmpty());
  }

  public void testRemove() {
    final Set<YTIdentifiable> expected = new HashSet<YTIdentifiable>(8);

    expected.add(new YTRecordId("#77:12"));
    expected.add(new YTRecordId("#77:13"));
    expected.add(new YTRecordId("#77:14"));
    expected.add(new YTRecordId("#77:15"));
    expected.add(new YTRecordId("#77:16"));

    final RidBag bag = new RidBag(database);
    assertEmbedded(bag.isEmbedded());
    bag.addAll(expected);
    assertEmbedded(bag.isEmbedded());

    bag.remove(new YTRecordId("#77:23"));
    assertEmbedded(bag.isEmbedded());

    final Set<YTIdentifiable> expectedTwo = new HashSet<YTIdentifiable>(8);
    expectedTwo.addAll(expected);

    for (YTIdentifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }

    Assert.assertTrue(expectedTwo.isEmpty());

    expected.remove(new YTRecordId("#77:14"));
    bag.remove(new YTRecordId("#77:14"));
    assertEmbedded(bag.isEmbedded());

    expectedTwo.addAll(expected);

    for (YTIdentifiable identifiable : bag) {
      assertTrue(expectedTwo.remove(identifiable));
    }
  }

  public void testSaveLoad() throws Exception {
    Set<YTIdentifiable> expected = new HashSet<YTIdentifiable>(8);

    expected.add(new YTRecordId("#77:12"));
    expected.add(new YTRecordId("#77:13"));
    expected.add(new YTRecordId("#77:14"));
    expected.add(new YTRecordId("#77:15"));
    expected.add(new YTRecordId("#77:16"));
    expected.add(new YTRecordId("#77:17"));
    expected.add(new YTRecordId("#77:18"));
    expected.add(new YTRecordId("#77:19"));
    expected.add(new YTRecordId("#77:20"));
    expected.add(new YTRecordId("#77:21"));
    expected.add(new YTRecordId("#77:22"));

    EntityImpl doc = new EntityImpl();

    final RidBag bag = new RidBag(database);
    bag.addAll(expected);

    doc.field("ridbag", bag);
    assertEmbedded(bag.isEmbedded());

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    final YTRID id = doc.getIdentity();

    database.close();

    database = createSessionInstance();

    doc = database.load(id);
    doc.setLazyLoad(false);

    final RidBag loaded = doc.field("ridbag");
    assertEmbedded(loaded.isEmbedded());

    Assert.assertEquals(loaded.size(), expected.size());
    for (YTIdentifiable identifiable : loaded) {
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

    HashSet<YTIdentifiable> result = new HashSet<YTIdentifiable>();

    for (YTIdentifiable oIdentifiable : ridBag) {
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
    List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();
    document.field("bag", bag);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTRID rid = document.getIdentity();

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

    Set<YTIdentifiable> docs = Collections.newSetFromMap(new IdentityHashMap<>());
    for (YTIdentifiable id : ridBag) {
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
    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docs.remove(identifiable.getRecord()));
      ridBag.remove(identifiable);
      Assert.assertEquals(ridBag.size(), docs.size());

      int counter = 0;
      for (YTIdentifiable id : ridBag) {
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

    List<YTIdentifiable> itemsToAdd = new ArrayList<>();

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

    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(itemsToAdd.remove(id));
    }

    Assert.assertTrue(itemsToAdd.isEmpty());
    database.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBack() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (database.getStorage() instanceof OStorageProxy) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
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

    List<YTIdentifiable> addedItems = new ArrayList<>();
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

    List<YTIdentifiable> addedItemsCopy = new ArrayList<>(addedItems);
    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (YTIdentifiable id : ridBag) {
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

    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());
    database.rollback();
  }

  public void testFromEmbeddedToSBTreeAndBackTx() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(7);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    if (database.isRemote()) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
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

    List<YTIdentifiable> addedItems = new ArrayList<YTIdentifiable>();

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

    List<YTIdentifiable> addedItemsCopy = new ArrayList<YTIdentifiable>(addedItems);
    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (YTIdentifiable id : ridBag) {
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

    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());

    ridBag = document.field("ridBag");
    Assert.assertFalse(ridBag.isEmbedded());

    addedItems.addAll(addedItemsCopy);
    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(addedItems.remove(id));
    }

    Assert.assertTrue(addedItems.isEmpty());
    database.rollback();
  }

  public void testRemoveSavedInCommit() {
    database.begin();
    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

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

    Iterator<YTIdentifiable> iterator = docsToAdd.listIterator(7);
    while (iterator.hasNext()) {
      YTIdentifiable docToAdd = iterator.next();
      ridBag.remove(docToAdd);
      iterator.remove();
    }

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    ridBag = document.field("ridBag");
    assertEmbedded(ridBag.isEmbedded());

    List<YTIdentifiable> docsToAddCopy = new ArrayList<YTIdentifiable>(docsToAdd);
    for (YTIdentifiable id : ridBag) {
      Assert.assertTrue(docsToAdd.remove(id));
    }

    Assert.assertTrue(docsToAdd.isEmpty());

    docsToAdd.addAll(docsToAddCopy);

    ridBag = document.field("ridBag");

    for (YTIdentifiable id : ridBag) {
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
    final List<YTIdentifiable> rids = new ArrayList<YTIdentifiable>();
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

    final List<YTIdentifiable> newDocs = new ArrayList<YTIdentifiable>();
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
        YTIdentifiable newDoc = newDocs.get(i);
        rids.remove(newDoc);
        ridBag.remove(newDoc);
        newDocs.remove(newDoc);

        size--;
      }
    }

    for (YTIdentifiable identifiable : ridBag) {
      if (newDocs.contains(identifiable) && rnd.nextBoolean()) {
        ridBag.remove(identifiable);
        rids.remove(identifiable);

        size--;
      }
    }

    Assert.assertEquals(ridBag.size(), size);
    List<YTIdentifiable> ridsCopy = new ArrayList<YTIdentifiable>(rids);

    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(rids.remove(identifiable));
    }

    Assert.assertTrue(rids.isEmpty());

    document.save();
    database.commit();

    document = database.load(document.getIdentity());
    ridBag = document.field("ridBag");

    rids.addAll(ridsCopy);
    for (YTIdentifiable identifiable : ridBag) {
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
        ODocumentHelper.hasSameContentOf(doc, database, testDocument, database, null));
    database.rollback();
  }

  protected abstract void assertEmbedded(boolean isEmbedded);

  private static void massiveInsertionIteration(Random rnd, List<YTIdentifiable> rids,
      RidBag bag) {
    Iterator<YTIdentifiable> bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      YTIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }

    Assert.assertEquals(bag.size(), rids.size());

    for (int i = 0; i < 100; i++) {
      if (rnd.nextDouble() < 0.2 & rids.size() > 5) {
        final int index = rnd.nextInt(rids.size());
        final YTIdentifiable rid = rids.remove(index);
        bag.remove(rid);
      } else {
        final long position;
        position = rnd.nextInt(300);

        final YTRecordId recordId = new YTRecordId(1, position);
        rids.add(recordId);
        bag.add(recordId);
      }
    }

    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final YTIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));

      if (rnd.nextDouble() < 0.05) {
        bagIterator.remove();
        Assert.assertTrue(rids.remove(bagValue));
      }
    }

    Assert.assertEquals(bag.size(), rids.size());
    bagIterator = bag.iterator();

    while (bagIterator.hasNext()) {
      final YTIdentifiable bagValue = bagIterator.next();
      Assert.assertTrue(rids.contains(bagValue));
    }
  }
}
