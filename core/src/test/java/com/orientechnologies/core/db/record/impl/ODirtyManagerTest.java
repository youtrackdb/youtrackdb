package com.orientechnologies.core.db.record.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.db.record.LinkSet;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.ODirtyManager;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class ODirtyManagerTest extends DBTestBase {

  public ODirtyManagerTest() {
  }

  @Test
  public void testBasic() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedDocument() {
    YTEntityImpl doc = new YTEntityImpl();
    YTEntityImpl doc1 = new YTEntityImpl();
    doc.setProperty("test", doc1, YTType.EMBEDDED);
    YTEntityImpl doc2 = new YTEntityImpl();
    doc1.setProperty("test2", doc2, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLink() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc.setProperty("test1", doc2, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testRemoveLink() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc.setProperty("test1", doc2, YTType.LINK);
    doc.removeProperty("test1");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testSetToNullLink() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc.setProperty("test1", doc2, YTType.LINK);
    doc.setProperty("test1", null);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkOther() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    YTEntityImpl doc1 = new YTEntityImpl();
    doc.setProperty("test1", doc1, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollection() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    lst.add(doc1);
    doc.field("list", lst);

    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl doc2 = new YTEntityImpl();
    set.add(doc2);
    doc.field("set", set);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionRemove() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    lst.add(doc1);
    doc.field("list", lst);
    doc.removeField("list");

    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl doc2 = new YTEntityImpl();
    set.add(doc2);
    doc.field("set", set);
    doc.removeField("set");

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionOther() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    lst.add(doc1);
    doc.field("list", lst);
    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl doc2 = new YTEntityImpl();
    set.add(doc2);
    doc.field("set", set);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    ODirtyManager manager2 = ORecordInternal.getDirtyManager(doc2);
    assertTrue(manager2.isSame(manager));
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMapOther() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    map.put("some", doc1);
    doc.field("list", map);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedMap() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();

    YTEntityImpl doc1 = new YTEntityImpl();
    map.put("bla", "bla");
    map.put("some", doc1);

    doc.field("list", map, YTType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedCollection() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");

    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    lst.add(doc1);
    doc.field("list", lst, YTType.EMBEDDEDLIST);

    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl doc2 = new YTEntityImpl();
    set.add(doc2);
    doc.field("set", set, YTType.EMBEDDEDSET);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testRidBag() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    RidBag bag = new RidBag(db);
    YTEntityImpl doc1 = new YTEntityImpl();
    bag.add(doc1);
    doc.field("bag", bag);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbendedWithEmbeddedCollection() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");

    YTEntityImpl emb = new YTEntityImpl();
    doc.setProperty("emb", emb, YTType.EMBEDDED);

    YTEntityImpl embedInList = new YTEntityImpl();
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    lst.add(embedInList);
    emb.setProperty("lst", lst, YTType.EMBEDDEDLIST);
    YTEntityImpl link = new YTEntityImpl();
    embedInList.setProperty("set", link, YTType.LINK);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);

    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleLevelEmbeddedCollection() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl embeddedInList = new YTEntityImpl();
    YTEntityImpl link = new YTEntityImpl();
    embeddedInList.setProperty("link", link, YTType.LINK);
    lst.add(embeddedInList);
    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl embeddedInSet = new YTEntityImpl();
    embeddedInSet.setProperty("list", lst, YTType.EMBEDDEDLIST);
    set.add(embeddedInSet);
    doc.setProperty("set", set, YTType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    ODirtyManager managerNested = ORecordInternal.getDirtyManager(embeddedInSet);
    assertTrue(manager.isSame(managerNested));
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionEmbedded() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl embeddedInList = new YTEntityImpl();
    YTEntityImpl link = new YTEntityImpl();
    embeddedInList.setProperty("link", link, YTType.LINK);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, YTType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleCollectionDocumentEmbedded() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl embeddedInList = new YTEntityImpl();
    YTEntityImpl link = new YTEntityImpl();
    YTEntityImpl embInDoc = new YTEntityImpl();
    embInDoc.setProperty("link", link, YTType.LINK);
    embeddedInList.setProperty("some", embInDoc, YTType.EMBEDDED);
    lst.add(embeddedInList);
    Set<Object> set = new HashSet<Object>();
    set.add(lst);
    doc.setProperty("set", set, YTType.EMBEDDEDSET);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleMapEmbedded() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("test", "ddd");
    List<YTEntityImpl> lst = new ArrayList<YTEntityImpl>();
    YTEntityImpl embeddedInList = new YTEntityImpl();
    YTEntityImpl link = new YTEntityImpl();
    embeddedInList.setProperty("link", link, YTType.LINK);
    lst.add(embeddedInList);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("some", lst);
    doc.setProperty("set", map, YTType.EMBEDDEDMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSet() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    YTEntityImpl link = new YTEntityImpl();
    set.add(link);
    doc.field("set", set);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSetNoConvert() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Set<YTIdentifiable> set = new LinkSet(doc);
    YTEntityImpl link = new YTEntityImpl();
    set.add(link);
    doc.field("set", set, YTType.LINKSET);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  @Ignore
  public void testLinkSetNoConvertRemove() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Set<YTIdentifiable> set = new LinkSet(doc);
    YTEntityImpl link = new YTEntityImpl();
    set.add(link);
    doc.field("set", set, YTType.LINKSET);
    doc.removeField("set");

    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkList() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    List<YTEntityImpl> list = new ArrayList<YTEntityImpl>();
    YTEntityImpl link = new YTEntityImpl();
    list.add(link);
    doc.field("list", list, YTType.LINKLIST);
    YTEntityImpl[] linkeds =
        new YTEntityImpl[]{
            new YTEntityImpl().field("name", "linked2"), new YTEntityImpl().field("name", "linked3")
        };
    doc.field("linkeds", linkeds, YTType.LINKLIST);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(4, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMap() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", "ddd");
    Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    YTEntityImpl link = new YTEntityImpl();
    map.put("bla", link);
    doc.field("map", map, YTType.LINKMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testNestedMapDocRidBag() {

    YTEntityImpl doc = new YTEntityImpl();

    Map<String, YTEntityImpl> embeddedMap = new HashMap<String, YTEntityImpl>();
    YTEntityImpl embeddedMapDoc = new YTEntityImpl();
    RidBag embeddedMapDocRidBag = new RidBag(db);
    YTEntityImpl link = new YTEntityImpl();
    embeddedMapDocRidBag.add(link);
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMap.put("k1", embeddedMapDoc);

    doc.field("embeddedMap", embeddedMap, YTType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }
}
