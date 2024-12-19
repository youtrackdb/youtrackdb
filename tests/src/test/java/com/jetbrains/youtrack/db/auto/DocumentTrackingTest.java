package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
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
public class DocumentTrackingTest extends BaseDBTest {

  @Parameters(value = "remote")
  public DocumentTrackingTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    if (!db.getMetadata().getSchema().existsClass("DocumentTrackingTestClass")) {
      final SchemaClass trackedClass =
          db.getMetadata().getSchema().createClass("DocumentTrackingTestClass");
      trackedClass.createProperty(db, "embeddedlist", PropertyType.EMBEDDEDLIST);
      trackedClass.createProperty(db, "embeddedmap", PropertyType.EMBEDDEDMAP);
      trackedClass.createProperty(db, "embeddedset", PropertyType.EMBEDDEDSET);
      trackedClass.createProperty(db, "linkset", PropertyType.LINKSET);
      trackedClass.createProperty(db, "linklist", PropertyType.LINKLIST);
      trackedClass.createProperty(db, "linkmap", PropertyType.LINKMAP);
    }
  }

  public void testDocumentEmbeddedListTrackingAfterSave() {
    EntityImpl document = ((EntityImpl) db.newEntity());

    final List<String> list = new ArrayList<>();
    list.add("value1");

    document.field("embeddedlist", list, PropertyType.EMBEDDEDLIST);
    document.field("val", 1);

    db.begin();
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    db.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSave() {
    EntityImpl document = ((EntityImpl) db.newEntity());

    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, PropertyType.EMBEDDEDMAP);
    document.field("val", 1);

    db.begin();
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSave() {
    EntityImpl document = ((EntityImpl) db.newEntity());

    final Set<String> set = new HashSet<>();
    set.add("value1");

    document.field("embeddedset", set, PropertyType.EMBEDDEDSET);
    document.field("val", 1);

    db.begin();
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    db.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSave() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final Set<RID> set = new HashSet<>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, PropertyType.LINKSET);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<RID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    db.rollback();
  }

  public void testDocumentLinkListTrackingAfterSave() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final List<RID> list = new ArrayList<>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, PropertyType.LINKLIST);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<RID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    db.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSave() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final Map<String, RID> map = new HashMap<>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, PropertyType.LINKMAP);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);

    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, RID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedListTrackingAfterSaveCacheDisabled() {
    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity());

    final List<String> list = new ArrayList<>();
    list.add("value1");

    document.field("embeddedlist", list, PropertyType.EMBEDDEDLIST);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    db.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveCacheDisabled() {
    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity());

    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");

    document.field("embeddedmap", map, PropertyType.EMBEDDEDMAP);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveCacheDisabled() {
    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity());

    final Set<String> set = new HashSet<>();
    set.add("value1");

    document.field("embeddedset", set, PropertyType.EMBEDDEDSET);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    db.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSaveCacheDisabled() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final Set<RID> set = new HashSet<>();
    set.add(docOne.getIdentity());

    document.field("linkset", set, PropertyType.LINKSET);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<RID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    db.rollback();
  }

  public void testDocumentLinkListTrackingAfterSaveCacheDisabled() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final List<RID> list = new ArrayList<>();
    list.add(docOne.getIdentity());

    document.field("linklist", list, PropertyType.LINKLIST);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<RID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    db.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSaveCacheDisabled() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity());

    final Map<String, RID> map = new HashMap<>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map, PropertyType.LINKMAP);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, RID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedListTrackingAfterSaveWitClass() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    db.begin();
    final List<String> list = new ArrayList<>();
    list.add("value1");

    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, 1, "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);
    Assert.assertTrue(document.isDirty());

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    db.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterSaveWithClass() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");

    db.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "key2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterSaveWithClass() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Set<String> set = new HashSet<>();
    set.add("value1");

    db.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNotNull(timeLine);

    Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, "value2", "value2"));

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    db.rollback();
  }

  public void testDocumentLinkSetTrackingAfterSaveWithClass() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Set<RID> set = new HashSet<>();
    set.add(docOne.getIdentity());

    document.field("linkset", set);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<RID> trackedSet = document.field("linkset");
    trackedSet.add(docTwo.getIdentity());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkset"});
    db.rollback();
  }

  public void testDocumentLinkListTrackingAfterSaveWithClass() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<RID> list = new ArrayList<>();
    list.add(docOne.getIdentity());

    document.field("linklist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final List<RID> trackedList = document.field("linklist");
    trackedList.add(docTwo.getIdentity());

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linklist"});
    db.rollback();
  }

  public void testDocumentLinkMapTrackingAfterSaveWithClass() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Map<String, RID> map = new HashMap<>();
    map.put("key1", docOne.getIdentity());

    document.field("linkmap", map);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Map<String, RID> trackedMap = document.field("linkmap");
    trackedMap.put("key2", docTwo.getIdentity());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
    Assert.assertNotNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"linkmap"});
    db.rollback();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedListTrackingAfterConversion() {
    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity());

    final Set<String> set = new HashSet<>();
    set.add("value1");

    document.field("embeddedlist", set);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist", PropertyType.EMBEDDEDLIST);
    trackedList.add("value2");
    db.rollback();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDocumentEmbeddedSetTrackingFailAfterConversion() {
    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity());

    final List<String> list = new ArrayList<>();
    list.add("value1");

    document.field("embeddedset", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset", PropertyType.EMBEDDEDSET);
    trackedSet.add("value2");
    db.rollback();
  }

  public void testDocumentEmbeddedListTrackingFailAfterReplace() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    final List<String> newTrackedList = new TrackedList<>(document);
    document.field("embeddedlist", newTrackedList);
    newTrackedList.add("value3");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    db.rollback();
  }

  public void testDocumentEmbeddedMapTrackingAfterReplace() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");

    db.begin();
    document.field("embeddedmap", map);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final Map<String, String> trackedMap = document.field("embeddedmap");
    trackedMap.put("key2", "value2");

    final Map<Object, String> newTrackedMap = new TrackedMap<>(document);
    document.field("embeddedmap", newTrackedMap);
    newTrackedMap.put("key3", "value3");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
    db.rollback();
  }

  public void testDocumentEmbeddedSetTrackingAfterReplace() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final Set<String> set = new HashSet<>();
    set.add("value1");

    db.begin();
    document.field("embeddedset", set);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertFalse(document.isDirty());
    Assert.assertEquals(document.getDirtyFields(), new String[]{});

    final Set<String> trackedSet = document.field("embeddedset");
    trackedSet.add("value2");

    final Set<String> newTrackedSet = new TrackedSet<>(document);
    document.field("embeddedset", newTrackedSet);
    newTrackedSet.add("value3");

    Assert.assertTrue(document.isDirty());

    final MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
    Assert.assertNull(timeLine);

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
    db.rollback();
  }

  public void testRemoveField() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.removeField("embeddedlist");

    Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    db.rollback();
  }

  public void testTrackingChangesSwitchedOff() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.setTrackingChanges(false);

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    db.rollback();
  }

  public void testTrackingChangesSwitchedOn() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
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

    final List<MultiValueChangeEvent> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent(ChangeType.ADD, 2, "value3"));

    MultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");

    Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);
    db.rollback();
  }

  public void testReset() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    db.rollback();
  }

  public void testClear() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    document.clear();

    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertTrue(document.isDirty());
    Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
    db.rollback();
  }

  public void testUnload() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    RecordInternal.unsetDirty(document);
    document.unload();

    Assert.assertFalse(document.isDirty());
    db.rollback();
  }

  public void testUnsetDirty() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.field("val", 1);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    Assert.assertEquals(document.getDirtyFields(), new String[]{});
    Assert.assertFalse(document.isDirty());

    final List<String> trackedList = document.field("embeddedlist");
    trackedList.add("value2");

    RecordInternal.unsetDirty(document);

    Assert.assertFalse(document.isDirty());
    db.rollback();
  }

  public void testRemoveFieldUsingIterator() {
    EntityImpl document = ((EntityImpl) db.newEntity("DocumentTrackingTestClass"));

    final List<String> list = new ArrayList<>();
    list.add("value1");

    db.begin();
    document.field("embeddedlist", list);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
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
    db.rollback();
  }
}
