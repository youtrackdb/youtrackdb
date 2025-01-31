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
    var entity = (EntityImpl) db.newEntity();
    Set objects = new HashSet();

    entity.field("objects", objects);
    entity.save();

    objects = entity.field("objects");
    Set subObjects = new HashSet();
    objects.add(subObjects);

    entity.save();
    db.commit();

    db.begin();

    entity = db.bindToSession(entity);
    orid = entity.getIdentity();
    objects = entity.field("objects");
    subObjects = (Set) objects.iterator().next();

    var nestedDoc = (EntityImpl) db.newEntity();
    subObjects.add(nestedDoc);

    entity.save();
    db.commit();

    entity = db.load(orid);
    objects = entity.field("objects");
    subObjects = (Set) objects.iterator().next();

    assertFalse(subObjects.isEmpty());
  }

  @Test
  public void testChangesValuesNestedTrackingSet() {

    db.begin();
    var document = (EntityImpl) db.newEntity();
    Set objects = new HashSet();

    document.field("objects", objects);
    Set subObjects = new HashSet();
    objects.add(subObjects);

    var nestedDoc = (EntityImpl) db.newEntity();
    subObjects.add(nestedDoc);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();
    subObjects.add("one");

    assertTrue(document.isDirty());
    var nestedTimiline =
        ((TrackedMultiValue<Object, Object>) subObjects).getTimeLine();
    assertEquals(1, nestedTimiline.getMultiValueChangeEvents().size());
    var multiValueChangeEvents =
        nestedTimiline.getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    db.commit();
  }

  @Test
  public void testChangesValuesNestedTrackingList() {

    db.begin();
    var document = (EntityImpl) db.newEntity();
    List objects = new ArrayList();

    document.field("objects", objects);
    List subObjects = new ArrayList();
    objects.add(subObjects);

    var nestedDoc = (EntityImpl) db.newEntity();
    subObjects.add(nestedDoc);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add(db.newEntity());

    assertTrue(document.isDirty());
    var multiValueChangeEvents =
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
    var document = (EntityImpl) db.newEntity();
    Map objects = new HashMap();

    document.field("objects", objects);
    Map subObjects = new HashMap();
    objects.put("first", subObjects);

    var nestedDoc = (EntityImpl) db.newEntity();
    subObjects.put("one", nestedDoc);

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", db.newEntity());

    assertTrue(document.isDirty());
    var multiValueChangeEvents =
        ((TrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof EntityImpl);
    db.commit();
  }
}
