package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
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
public class DocumentTrackingNestedCollectionsTest extends DbTestBase {

  @Test
  public void testTrackingNestedSet() {

    db.begin();
    RID orid;
    EntityImpl document = (EntityImpl) db.newEntity();
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

    EntityImpl nestedDoc = (EntityImpl) db.newEntity();
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
    EntityImpl document = (EntityImpl) db.newEntity();
    Set objects = new HashSet();

    document.field("objects", objects);
    Set subObjects = new HashSet();
    objects.add(subObjects);

    EntityImpl nestedDoc = (EntityImpl) db.newEntity();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();
    subObjects.add("one");

    assertTrue(document.isDirty());
    MultiValueChangeTimeLine<Object, Object> nestedTimiline =
        ((TrackedMultiValue<Object, Object>) subObjects).getTimeLine();
    assertEquals(1, nestedTimiline.getMultiValueChangeEvents().size());
    List<MultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        nestedTimiline.getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingList() {

    db.begin();
    EntityImpl document = (EntityImpl) db.newEntity();
    List objects = new ArrayList();

    document.field("objects", objects);
    List subObjects = new ArrayList();
    objects.add(subObjects);

    EntityImpl nestedDoc = (EntityImpl) db.newEntity();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add((EntityImpl) db.newEntity());

    assertTrue(document.isDirty());
    List<MultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((TrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals(1, multiValueChangeEvents.get(0).getKey());
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    assertEquals(2, multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof EntityImpl);
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingMap() {
    db.begin();
    EntityImpl document = (EntityImpl) db.newEntity();
    Map objects = new HashMap();

    document.field("objects", objects);
    Map subObjects = new HashMap();
    objects.put("first", subObjects);

    EntityImpl nestedDoc = (EntityImpl) db.newEntity();
    subObjects.put("one", nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", (EntityImpl) db.newEntity());

    assertTrue(document.isDirty());
    List<MultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((TrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof EntityImpl);
    db.commit();
  }
}
