package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
    EntityImpl document = new EntityImpl();
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

    EntityImpl nestedDoc = new EntityImpl();
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
    EntityImpl document = new EntityImpl();
    Set objects = new HashSet();

    document.field("objects", objects);
    Set subObjects = new HashSet();
    objects.add(subObjects);

    EntityImpl nestedDoc = new EntityImpl();
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
    EntityImpl document = new EntityImpl();
    List objects = new ArrayList();

    document.field("objects", objects);
    List subObjects = new ArrayList();
    objects.add(subObjects);

    EntityImpl nestedDoc = new EntityImpl();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add(new EntityImpl());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals(1, multiValueChangeEvents.get(0).getKey());
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    assertEquals(2, multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof EntityImpl);
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingMap() {
    db.begin();
    EntityImpl document = new EntityImpl();
    Map objects = new HashMap();

    document.field("objects", objects);
    Map subObjects = new HashMap();
    objects.put("first", subObjects);

    EntityImpl nestedDoc = new EntityImpl();
    subObjects.put("one", nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", new EntityImpl());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof EntityImpl);
    db.commit();
  }
}
