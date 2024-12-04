package com.orientechnologies.orient.core.db.record.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
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
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedDocument() {
    YTDocument doc = new YTDocument();
    YTDocument doc1 = new YTDocument();
    doc.setProperty("test", doc1, YTType.EMBEDDED);
    YTDocument doc2 = new YTDocument();
    doc1.setProperty("test2", doc2, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLink() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    YTDocument doc2 = new YTDocument();
    doc.setProperty("test1", doc2, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testRemoveLink() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    YTDocument doc2 = new YTDocument();
    doc.setProperty("test1", doc2, YTType.LINK);
    doc.removeProperty("test1");
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testSetToNullLink() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    YTDocument doc2 = new YTDocument();
    doc.setProperty("test1", doc2, YTType.LINK);
    doc.setProperty("test1", null);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkOther() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    YTDocument doc1 = new YTDocument();
    doc.setProperty("test1", doc1, YTType.LINK);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollection() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument doc1 = new YTDocument();
    lst.add(doc1);
    doc.field("list", lst);

    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument doc2 = new YTDocument();
    set.add(doc2);
    doc.field("set", set);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(3, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionRemove() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument doc1 = new YTDocument();
    lst.add(doc1);
    doc.field("list", lst);
    doc.removeField("list");

    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument doc2 = new YTDocument();
    set.add(doc2);
    doc.field("set", set);
    doc.removeField("set");

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testLinkCollectionOther() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument doc1 = new YTDocument();
    lst.add(doc1);
    doc.field("list", lst);
    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument doc2 = new YTDocument();
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
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Map<String, YTDocument> map = new HashMap<String, YTDocument>();
    YTDocument doc1 = new YTDocument();
    map.put("some", doc1);
    doc.field("list", map);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedMap() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Map<String, Object> map = new HashMap<String, Object>();

    YTDocument doc1 = new YTDocument();
    map.put("bla", "bla");
    map.put("some", doc1);

    doc.field("list", map, YTType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testEmbeddedCollection() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");

    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument doc1 = new YTDocument();
    lst.add(doc1);
    doc.field("list", lst, YTType.EMBEDDEDLIST);

    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument doc2 = new YTDocument();
    set.add(doc2);
    doc.field("set", set, YTType.EMBEDDEDSET);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(1, manager.getNewRecords().size());
  }

  @Test
  public void testRidBag() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    ORidBag bag = new ORidBag(db);
    YTDocument doc1 = new YTDocument();
    bag.add(doc1);
    doc.field("bag", bag);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testEmbendedWithEmbeddedCollection() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");

    YTDocument emb = new YTDocument();
    doc.setProperty("emb", emb, YTType.EMBEDDED);

    YTDocument embedInList = new YTDocument();
    List<YTDocument> lst = new ArrayList<YTDocument>();
    lst.add(embedInList);
    emb.setProperty("lst", lst, YTType.EMBEDDEDLIST);
    YTDocument link = new YTDocument();
    embedInList.setProperty("set", link, YTType.LINK);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);

    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testDoubleLevelEmbeddedCollection() {
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument embeddedInList = new YTDocument();
    YTDocument link = new YTDocument();
    embeddedInList.setProperty("link", link, YTType.LINK);
    lst.add(embeddedInList);
    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument embeddedInSet = new YTDocument();
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
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument embeddedInList = new YTDocument();
    YTDocument link = new YTDocument();
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
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument embeddedInList = new YTDocument();
    YTDocument link = new YTDocument();
    YTDocument embInDoc = new YTDocument();
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
    YTDocument doc = new YTDocument();
    doc.setProperty("test", "ddd");
    List<YTDocument> lst = new ArrayList<YTDocument>();
    YTDocument embeddedInList = new YTDocument();
    YTDocument link = new YTDocument();
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
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Set<YTDocument> set = new HashSet<YTDocument>();
    YTDocument link = new YTDocument();
    set.add(link);
    doc.field("set", set);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkSetNoConvert() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Set<YTIdentifiable> set = new OSet(doc);
    YTDocument link = new YTDocument();
    set.add(link);
    doc.field("set", set, YTType.LINKSET);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  @Ignore
  public void testLinkSetNoConvertRemove() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Set<YTIdentifiable> set = new OSet(doc);
    YTDocument link = new YTDocument();
    set.add(link);
    doc.field("set", set, YTType.LINKSET);
    doc.removeField("set");

    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testLinkList() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    List<YTDocument> list = new ArrayList<YTDocument>();
    YTDocument link = new YTDocument();
    list.add(link);
    doc.field("list", list, YTType.LINKLIST);
    YTDocument[] linkeds =
        new YTDocument[]{
            new YTDocument().field("name", "linked2"), new YTDocument().field("name", "linked3")
        };
    doc.field("linkeds", linkeds, YTType.LINKLIST);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(4, manager.getNewRecords().size());
  }

  @Test
  public void testLinkMap() {
    YTDocument doc = new YTDocument();
    doc.field("test", "ddd");
    Map<String, YTDocument> map = new HashMap<String, YTDocument>();
    YTDocument link = new YTDocument();
    map.put("bla", link);
    doc.field("map", map, YTType.LINKMAP);
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }

  @Test
  public void testNestedMapDocRidBag() {

    YTDocument doc = new YTDocument();

    Map<String, YTDocument> embeddedMap = new HashMap<String, YTDocument>();
    YTDocument embeddedMapDoc = new YTDocument();
    ORidBag embeddedMapDocRidBag = new ORidBag(db);
    YTDocument link = new YTDocument();
    embeddedMapDocRidBag.add(link);
    embeddedMapDoc.field("ridBag", embeddedMapDocRidBag);
    embeddedMap.put("k1", embeddedMapDoc);

    doc.field("embeddedMap", embeddedMap, YTType.EMBEDDEDMAP);

    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
    ODirtyManager manager = ORecordInternal.getDirtyManager(doc);
    assertEquals(2, manager.getNewRecords().size());
  }
}
