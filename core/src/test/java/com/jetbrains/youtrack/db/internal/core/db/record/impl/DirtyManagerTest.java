package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DirtyManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentInternal;
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
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedDocument() {
    EntityImpl doc = new EntityImpl();
    EntityImpl doc1 = new EntityImpl();
    doc.setProperty("test", doc1, PropertyType.EMBEDDED);
    EntityImpl doc2 = new EntityImpl();
    doc1.setProperty("test2", doc2, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLink() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = new EntityImpl();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testRemoveLink() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = new EntityImpl();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    doc.removeProperty("test1");
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testSetToNullLink() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    EntityImpl doc2 = new EntityImpl();
    doc.setProperty("test1", doc2, PropertyType.LINK);
    doc.setProperty("test1", null);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkOther() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    EntityImpl doc1 = new EntityImpl();
    doc.setProperty("test1", doc1, PropertyType.LINK);
    DirtyManager manager = RecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollection() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    lst.add(doc1);
    doc.field("list", lst);

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = new EntityImpl();
    set.add(doc2);
    doc.field("set", set);

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionRemove() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    lst.add(doc1);
    doc.field("list", lst);
    doc.removeField("list");

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = new EntityImpl();
    set.add(doc2);
    doc.field("set", set);
    doc.removeField("set");

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionOther() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    lst.add(doc1);
    doc.field("list", lst);
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = new EntityImpl();
    set.add(doc2);
    doc.field("set", set);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc1);
    DirtyManager manager2 = RecordInternal.getDirtyManager(doc2);
    assertTrue(manager2.isSame(manager));
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMapOther() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    map.put("some", doc1);
    doc.field("list", map);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedMap() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();

    EntityImpl doc1 = new EntityImpl();
    map.put("bla", "bla");
    map.put("some", doc1);

    doc.field("list", map, PropertyType.EMBEDDEDMAP);

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedCollection() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");

    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    lst.add(doc1);
    doc.field("list", lst, PropertyType.EMBEDDEDLIST);

    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl doc2 = new EntityImpl();
    set.add(doc2);
    doc.field("set", set, PropertyType.EMBEDDEDSET);

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testRidBag() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    RidBag bag = new RidBag(db);
    EntityImpl doc1 = new EntityImpl();
    bag.add(doc1);
    doc.field("bag", bag);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbendedWithEmbeddedCollection() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");

    EntityImpl emb = new EntityImpl();
    doc.setProperty("emb", emb, PropertyType.EMBEDDED);

    EntityImpl embedInList = new EntityImpl();
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    lst.add(embedInList);
    emb.setProperty("lst", lst, PropertyType.EMBEDDEDLIST);
    EntityImpl link = new EntityImpl();
    embedInList.setProperty("set", link, PropertyType.LINK);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);

    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleLevelEmbeddedCollection() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = new EntityImpl();
    EntityImpl link = new EntityImpl();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl embeddedInSet = new EntityImpl();
    embeddedInSet.setProperty("list", lst, PropertyType.EMBEDDEDLIST);
    set.add(embeddedInSet);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    DirtyManager managerNested = RecordInternal.getDirtyManager(embeddedInSet);
    assertTrue(manager.isSame(managerNested));
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionEmbedded() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = new EntityImpl();
    EntityImpl link = new EntityImpl();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionDocumentEmbedded() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = new EntityImpl();
    EntityImpl link = new EntityImpl();
    EntityImpl embInDoc = new EntityImpl();
    embInDoc.setProperty("link", link, PropertyType.LINK);
    embeddedInList.setProperty("some", embInDoc, PropertyType.EMBEDDED);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleMapEmbedded() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("test", "ddd");
    List<EntityImpl> lst = new ArrayList<EntityImpl>();
    EntityImpl embeddedInList = new EntityImpl();
    EntityImpl link = new EntityImpl();
    embeddedInList.setProperty("link", link, PropertyType.LINK);
    lst.add(embeddedInList);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("some", lst);
    doc.setProperty("set", map, PropertyType.EMBEDDEDMAP);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSet() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Set<EntityImpl> set = new HashSet<EntityImpl>();
    EntityImpl link = new EntityImpl();
    set.add(link);
    doc.field("set", set);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSetNoConvert() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Set<Identifiable> set = new LinkSet(doc);
    EntityImpl link = new EntityImpl();
    set.add(link);
    doc.field("set", set, PropertyType.LINKSET);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  @Ignore
  public void testLinkSetNoConvertRemove() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Set<Identifiable> set = new LinkSet(doc);
    EntityImpl link = new EntityImpl();
    set.add(link);
    doc.field("set", set, PropertyType.LINKSET);
    doc.removeField("set");

    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkList() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    List<EntityImpl> list = new ArrayList<EntityImpl>();
    EntityImpl link = new EntityImpl();
    list.add(link);
    doc.field("list", list, PropertyType.LINKLIST);
    EntityImpl[] linkeds =
        new EntityImpl[]{
            new EntityImpl().field("name", "linked2"), new EntityImpl().field("name", "linked3")
        };
    doc.field("linkeds", linkeds, PropertyType.LINKLIST);

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(4, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMap() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", "ddd");
    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    EntityImpl link = new EntityImpl();
    map.put("bla", link);
    doc.field("map", map, PropertyType.LINKMAP);
    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testNestedMapDocRidBag() {

    EntityImpl doc = new EntityImpl();

    Map<String, EntityImpl> embeddedMap = new HashMap<String, EntityImpl>();
    EntityImpl embeddedMapDoc = new EntityImpl();
    RidBag embeddedMapDocRidBag = new RidBag(db);
    EntityImpl link = new EntityImpl();
    embeddedMapDocRidBag.add(link);
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMap.put("k1", embeddedMapDoc);

    doc.field("embeddedMap", embeddedMap, PropertyType.EMBEDDEDMAP);

    DocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    DirtyManager manager = RecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }
}
