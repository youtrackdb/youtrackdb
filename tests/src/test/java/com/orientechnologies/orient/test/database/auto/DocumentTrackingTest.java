package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
      final OClass trackedClass =
          database.getMetadata().getSchema().createClass("DocumentTrackingTestClass");
      trackedClass.createProperty(database, "embeddedlist", OType.EMBEDDEDLIST);
      trackedClass.createProperty(database, "embeddedmap", OType.EMBEDDEDMAP);
      trackedClass.createProperty(database, "embeddedset", OType.EMBEDDEDSET);
      trackedClass.createProperty(database, "linkset", OType.LINKSET);
      trackedClass.createProperty(database, "linklist", OType.LINKLIST);
      trackedClass.createProperty(database, "linkmap", OType.LINKMAP);
    }
  }

  public void testDocumentEmbeddedListTrackingAfterSave() {
    ODocument document = new ODocument();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list, OType.EMBEDDEDLIST);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentEmbeddedMapTrackingAfterSave() {
    ODocument document = new ODocument();

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, OType.EMBEDDEDMAP);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentEmbeddedSetTrackingAfterSave() {
    ODocument document = new ODocument();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedset", set, OType.EMBEDDEDSET);
    document.field("val", 1);

    database.begin();
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentLinkSetTrackingAfterSave() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final Set<ORID> set = new HashSet<ORID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, OType.LINKSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<ORID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
  }

  public void testDocumentLinkListTrackingAfterSave() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final List<ORID> list = new ArrayList<ORID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, OType.LINKLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<ORID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
  }

  public void testDocumentLinkMapTrackingAfterSave() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final Map<String, ORID> map = new HashMap<String, ORID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, OType.LINKMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, ORID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
  }

  public void testDocumentEmbeddedListTrackingAfterSaveCacheDisabled() {
    database.begin();
    ODocument document = new ODocument();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list, OType.EMBEDDEDLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveCacheDisabled() {
    database.begin();
    ODocument document = new ODocument();

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, OType.EMBEDDEDMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveCacheDisabled() {
    database.begin();
    ODocument document = new ODocument();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedset", set, OType.EMBEDDEDSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

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
  }

  public void testDocumentLinkSetTrackingAfterSaveCacheDisabled() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final Set<ORID> set = new HashSet<ORID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, OType.LINKSET);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<ORID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
  }

  public void testDocumentLinkListTrackingAfterSaveCacheDisabled() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final List<ORID> list = new ArrayList<ORID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, OType.LINKLIST);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<ORID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
  }

  public void testDocumentLinkMapTrackingAfterSaveCacheDisabled() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument();

    final Map<String, ORID> map = new HashMap<String, ORID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, OType.LINKMAP);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, ORID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
  }

  public void testDocumentEmbeddedListTrackingAfterSaveWitClass() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    database.begin();
    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

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
    Assert.assertTrue(document.isDirty());

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveWithClass() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    database.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

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
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveWithClass() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    database.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    database.commit();

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
  }

  public void testDocumentLinkSetTrackingAfterSaveWithClass() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Set<ORID> set = new HashSet<ORID>();
    set.add(docOne.getIdentity());

    document.field("linkset", set);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<ORID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
  }

  public void testDocumentLinkListTrackingAfterSaveWithClass() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<ORID> list = new ArrayList<ORID>();
    list.add(docOne.getIdentity());

    document.field("linklist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<ORID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
  }

  public void testDocumentLinkMapTrackingAfterSaveWithClass() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Map<String, ORID> map = new HashMap<String, ORID>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, ORID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedListTrackingAfterConversion() {
    database.begin();
    ODocument document = new ODocument();

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    document.field("embeddedlist", set);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist", OType.EMBEDDEDLIST);
    trackedList.add("value2");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedSetTrackingFailAfterConversion() {
    database.begin();
    ODocument document = new ODocument();

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    document.field("embeddedset", list);
    document.field("val", 1);
    document.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset", OType.EMBEDDEDSET);
    trackedSet.add("value2");
  }

  public void testDocumentEmbeddedListTrackingFailAfterReplace() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    final List<String> newTrackedList = new OTrackedList<String>(document);
    document.field("embeddedlist", newTrackedList);
    newTrackedList.add("value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
  }

  public void testDocumentEmbeddedMapTrackingAfterReplace() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Map<String, String> map = new HashMap<String, String>();
    map.put("key1", "value1");

    database.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    final Map<Object, String> newTrackedMap = new OTrackedMap<String>(document);
    document.field("embeddedmap", newTrackedMap);
    newTrackedMap.put("key3", "value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
  }

  public void testDocumentEmbeddedSetTrackingAfterReplace() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final Set<String> set = new HashSet<String>();
    set.add("value1");

    database.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    final Set<String> newTrackedSet = new OTrackedSet<String>(document);
    document.field("embeddedset", newTrackedSet);
    newTrackedSet.add("value3");

    Assert.assertTrue(document.isDirty());

    final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
  }

  public void testRemoveField() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.removeField("embeddedlist");

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
  }

  public void testTrackingChangesSwitchedOff() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.setTrackingChanges(false);

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
  }

  public void testTrackingChangesSwitchedOn() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

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
  }

  public void testReset() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document = new ODocument("DocumentTrackingTestClass");

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
  }

  public void testClear() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.clear();

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
  }

  public void testUnload() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(document);
    document.unload();

    Assert.assertFalse(document.isDirty());
  }

  public void testUnsetDirty() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    database.commit();

    document = database.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(document);

    Assert.assertFalse(document.isDirty());
  }

  public void testRemoveFieldUsingIterator() {
    ODocument document = new ODocument("DocumentTrackingTestClass");

    final List<String> list = new ArrayList<String>();
    list.add("value1");

    database.begin();
    document.field("embeddedlist", list);
    document.save();
    database.commit();

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
  }
}
