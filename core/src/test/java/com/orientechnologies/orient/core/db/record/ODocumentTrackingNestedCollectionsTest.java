package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/**
 *
 */
public class ODocumentTrackingNestedCollectionsTest extends DBTestBase {

  @Test
  public void testTrackingNestedSet() {

    db.begin();
    YTRID orid;
    YTDocument document = new YTDocument();
    Set objects = new HashSet();

    document.field("objects", objects);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    objects = document.field("objects");
    Set subObjects = new HashSet();
    objects.add(subObjects);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    document = db.bindToSession(document);
    orid = document.getIdentity();
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    YTDocument nestedDoc = new YTDocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    document = db.load(orid);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    assertFalse(subObjects.isEmpty());
  }

  @Test
  public void testChangesValuesNestedTrackingSet() {

    db.begin();
    YTDocument document = new YTDocument();
    Set objects = new HashSet();

    document.field("objects", objects);
    Set subObjects = new HashSet();
    objects.add(subObjects);

    YTDocument nestedDoc = new YTDocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();
    subObjects.add("one");

    assertTrue(document.isDirty());
    OMultiValueChangeTimeLine<Object, Object> nestedTimiline =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine();
    assertEquals(1, nestedTimiline.getMultiValueChangeEvents().size());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        nestedTimiline.getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingList() {

    db.begin();
    YTDocument document = new YTDocument();
    List objects = new ArrayList();

    document.field("objects", objects);
    List subObjects = new ArrayList();
    objects.add(subObjects);

    YTDocument nestedDoc = new YTDocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add(new YTDocument());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals(1, multiValueChangeEvents.get(0).getKey());
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    assertEquals(2, multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof YTDocument);
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingMap() {
    db.begin();
    YTDocument document = new YTDocument();
    Map objects = new HashMap();

    document.field("objects", objects);
    Map subObjects = new HashMap();
    objects.put("first", subObjects);

    YTDocument nestedDoc = new YTDocument();
    subObjects.put("one", nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", new YTDocument());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof YTDocument);
    db.commit();
  }
}
