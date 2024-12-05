package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.db.record.OMultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.OMultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DocumentTrackingTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public DocumentTrackingTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    if (!database.getMetadata().getSchema().existsClass("DocumentTrackingTestClass")) {
      final YTClass trackedClass =
          database.getMetadata().getSchema().createClass("DocumentTrackingTestClass");
      trackedClass.createProperty(database, "embeddedlist", YTType.EMBEDDEDLIST);
      trackedClass.createProperty(database, "embeddedmap", YTType.EMBEDDEDMAP);
      trackedClass.createProperty(database, "embeddedset", YTType.EMBEDDEDSET);
      trackedClass.createProperty(database, "linkset", YTType.LINKSET);
      trackedClass.createProperty(database, "linklist", YTType.LINKLIST);
      trackedClass.createProperty(database, "linkmap", YTType.LINKMAP);
    }
  }

  public void testDocumentEmbeddedListTrackingAfterSave() {
    EntityImpl document = new EntityImpl();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list, YTType.EMBEDDEDLIST);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    database.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSave() {
    EntityImpl document = new EntityImpl();

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, YTType.EMBEDDEDMAP);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSave() {
    EntityImpl document = new EntityImpl();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedset", set, YTType.EMBEDDEDSET);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    database.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSave() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final Set<YTRID> set = new HashSet<YTRID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, YTType.LINKSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<YTRID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    database.rollback();
  }

  public void testDocumentLinkListTrackingAfterSave() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final List<YTRID> list = new ArrayList<YTRID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, YTType.LINKLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<YTRID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    database.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSave() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final Map<String, YTRID> map = new HashMap<String, YTRID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, YTType.LINKMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, YTRID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedListTrackingAfterSaveCacheDisabled() {
    database.begin();
    EntityImpl document = new EntityImpl();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list, YTType.EMBEDDEDLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    database.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveCacheDisabled() {
    database.begin();
    EntityImpl document = new EntityImpl();

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, YTType.EMBEDDEDMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveCacheDisabled() {
    database.begin();
    EntityImpl document = new EntityImpl();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedset", set, YTType.EMBEDDEDSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    database.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSaveCacheDisabled() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final Set<YTRID> set = new HashSet<YTRID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, YTType.LINKSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<YTRID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    database.rollback();
  }

  public void testDocumentLinkListTrackingAfterSaveCacheDisabled() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final List<YTRID> list = new ArrayList<YTRID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, YTType.LINKLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<YTRID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    database.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSaveCacheDisabled() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl();

    final Map<String, YTRID> map = new HashMap<String, YTRID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, YTType.LINKMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, YTRID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedListTrackingAfterSaveWitClass() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    database.begin();
    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);
    Assert.assertTrue(document.isDirty());

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    database.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveWithClass() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    database.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveWithClass() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    database.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    database.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSaveWithClass() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Set<YTRID> set = new HashSet<YTRID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<YTRID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    database.rollback();
  }

  public void testDocumentLinkListTrackingAfterSaveWithClass() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<YTRID> list = new ArrayList<YTRID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<YTRID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    database.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSaveWithClass() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Map<String, YTRID> map = new HashMap<String, YTRID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, YTRID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    database.rollback();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedListTrackingAfterConversion() {
    database.begin();
    EntityImpl document = new EntityImpl();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedlist", set);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist", YTType.EMBEDDEDLIST);
    trackedList.add("value2");
    database.rollback();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedSetTrackingFailAfterConversion() {
    database.begin();
    EntityImpl document = new EntityImpl();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedset", list);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset", YTType.EMBEDDEDSET);
    trackedSet.add("value2");
    database.rollback();
  }

  public void testDocumentEmbeddedListTrackingFailAfterReplace() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    final List<String> newTrackedList = new TrackedList<String>(document);
    document.field("embeddedlist", newTrackedList);
    newTrackedList.add("value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    database.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterReplace() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    database.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    final Map<Object, String> newTrackedMap = new TrackedMap<String>(document);
    document.field("embeddedmap", newTrackedMap);
    newTrackedMap.put("key3", "value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    database.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterReplace() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    database.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    final Set<String> newTrackedSet = new TrackedSet<String>(document);
    document.field("embeddedset", newTrackedSet);
    newTrackedSet.add("value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    database.rollback();
  }

  public void testRemoveField() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.removeField("embeddedlist");

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    database.rollback();
  }

  public void testTrackingChangesSwitchedOff() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.setTrackingChanges(false);

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    database.rollback();
  }

  public void testTrackingChangesSwitchedOn() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.setTrackingChanges(false);
    document.setTrackingChanges(true);

    trackedList.add("value3");

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    Assert.assertTrue(document.isDirty());
    Assert.assertNotNull(document.getCollectionTimeLine("embeddedlist"));

    final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
    firedEvents.add(
        new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 2, "value3"));

    OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);
    database.rollback();
  }

  public void testReset() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document = new EntityImpl("DocumentTrackingTestClass");

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    database.rollback();
  }

  public void testClear() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.clear();

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    database.rollback();
  }

  public void testUnload() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(document);
    document.unload();

    Assert.assertFalse(document.isDirty());
    database.rollback();
  }

  public void testUnsetDirty() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(document);

    Assert.assertFalse(document.isDirty());
    database.rollback();
  }

  public void testRemoveFieldUsingIterator() {
    EntityImpl document = new EntityImpl("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    final Iterator fieldIterator = document.iterator();
    fieldIterator.next();
    fieldIterator.remove();

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    database.rollback();
  }
}
