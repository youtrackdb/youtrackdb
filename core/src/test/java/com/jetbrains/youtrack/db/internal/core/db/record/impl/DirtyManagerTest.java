package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.DirtyManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class DirtyManagerTest extends DbTestBase {

  public DirtyManagerTest() {
  }

  @Test
  public void testBasic() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedDocument() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    doc.setProperty("test", doc1, PropertyType.EMBEDDED);
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    doc1.setProperty("test2", doc2, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLink() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testRemoveLink() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    doc.removeProperty("test1");
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testSetToNullLink() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    doc.setProperty("test1", null);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkOther() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    doc.setProperty("test1", doc1, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollection() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    lst.add(doc1);
    doc.field("list", lst);

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    set.add(doc2);
    doc.field("set", set);

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionRemove() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    lst.add(doc1);
    doc.field("list", lst);
    doc.removeField("list");

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    set.add(doc2);
    doc.field("set", set);
    doc.removeField("set");

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionOther() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    lst.add(doc1);
    doc.field("list", lst);
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    set.add(doc2);
    doc.field("set", set);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc1);
    DirtyManager manager2 = RecordInternal.getDirtyManager(db, doc2);
    assertTrue(manager2.isSame(manager));
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMapOther() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    map.put("some", doc1);
    doc.field("list", map);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedMap() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();

    EntityImpl doc1 = (EntityImpl) db.newEntity();
    map.put("bla", "bla");
    map.put("some", doc1);

    doc.field("list", map, PropertyType.EMBEDDEDMAP);

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedCollection() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");

    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    lst.add(doc1);
    doc.field("list", lst, PropertyType.EMBEDDEDLIST);

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = (EntityImpl) db.newEntity();
    set.add(doc2);
    doc.field("set", set, PropertyType.EMBEDDEDSET);

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testRidBag() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    RidBag bag = new RidBag(db);
    EntityImpl doc1 = (EntityImpl) db.newEntity();
    bag.add(doc1.getIdentity());
    doc.field("bag", bag);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbendedWithEmbeddedCollection() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");

    EntityImpl emb = (EntityImpl) db.newEntity();
    doc.setProperty("emb", emb, PropertyType.EMBEDDED);

    EntityImpl embedInList = (EntityImpl) db.newEntity();
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    lst.add(embedInList);
    emb.setProperty("lst", lst, PropertyType.EMBEDDEDLIST);
    EntityImpl link = (EntityImpl) db.newEntity();
    embedInList.setProperty("set", link, PropertyType.LINK);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);

    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleLevelEmbeddedCollection() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = (EntityImpl) db.newEntity();
    EntityImpl link = (EntityImpl) db.newEntity();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl embeddedInSet = (EntityImpl) db.newEntity();
    embeddedInSet.setProperty("list", lst, PropertyType.EMBEDDEDLIST);
    set.add(embeddedInSet);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    DirtyManager managerNested = RecordInternal.getDirtyManager(db, embeddedInSet);
    assertTrue(manager.isSame(managerNested));
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionEmbedded() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = (EntityImpl) db.newEntity();
    EntityImpl link = (EntityImpl) db.newEntity();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionDocumentEmbedded() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = (EntityImpl) db.newEntity();
    EntityImpl link = (EntityImpl) db.newEntity();
    EntityImpl embInDoc = (EntityImpl) db.newEntity();
    embInDoc.setProperty("link", link, PropertyType.LINK);
    embeddedInList.setProperty("some", embInDoc, PropertyType.EMBEDDED);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleMapEmbedded() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = (EntityImpl) db.newEntity();
    EntityImpl link = (EntityImpl) db.newEntity();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("some", lst);
    doc.setProperty("set", map, PropertyType.EMBEDDEDMAP);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSet() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl link = (EntityImpl) db.newEntity();
    set.add(link);
    doc.field("set", set);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSetNoConvert() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Set<Identifiable> set = new LinkSet(doc);
    EntityImpl link = (EntityImpl) db.newEntity();
    set.add(link);
    doc.field("set", set, PropertyType.LINKSET);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  @Ignore
  public void testLinkSetNoConvertRemove() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Set<Identifiable> set = new LinkSet(doc);
    EntityImpl link = (EntityImpl) db.newEntity();
    set.add(link);
    doc.field("set", set, PropertyType.LINKSET);
    doc.removeField("set");

    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkList() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    List<EntityImpl> list = new ArrayList<EntityImpl>();
    EntityImpl link = (EntityImpl) db.newEntity();
    list.add(link);
    doc.field("list", list, PropertyType.LINKLIST);
    EntityImpl[] linkeds =
        new EntityImpl[]{
            ((EntityImpl) db.newEntity()).field("name", "linked2"),
            ((EntityImpl) db.newEntity()).field("name", "linked3")
        };
    doc.field("linkeds", linkeds, PropertyType.LINKLIST);

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(4, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMap() {
    EntityImpl doc = (EntityImpl) db.newEntity();
    doc.field("test", "ddd");
    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    EntityImpl link = (EntityImpl) db.newEntity();
    map.put("bla", link);
    doc.field("map", map, PropertyType.LINKMAP);
    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testNestedMapDocRidBag() {
    EntityImpl doc = (EntityImpl) db.newEntity();

    Map<String, EntityImpl> embeddedMap = new HashMap<String, EntityImpl>();
    EntityImpl embeddedMapDoc = (EntityImpl) db.newEntity();
    RidBag embeddedMapDocRidBag = new RidBag(db);
    EntityImpl link = (EntityImpl) db.newEntity();
    embeddedMapDocRidBag.add(link.getIdentity());
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMap.put("k1", embeddedMapDoc);

    doc.field("embeddedMap", embeddedMap, PropertyType.EMBEDDEDMAP);

    EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(db, doc);
    assertEquals(2, manager.getNewRecords().size());
  }
}
