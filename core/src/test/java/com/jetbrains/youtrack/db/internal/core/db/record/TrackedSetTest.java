package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TrackedSetTest extends DbTestBase {

  @Test
  public void testAddOne() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.ADD, "value1", "value1", null);
    trackedSet.add("value1");
    Assert.assertEquals(event, trackedSet.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testAddTwo() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    doc.setPropertyInternal("tracked", trackedSet);
    trackedSet.add("value1");
    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testAddThree() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    trackedSet.addInternal("value1");

    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testAddFour() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);

    trackedSet.add("value1");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.disableTracking(doc);
    trackedSet.enableTracking(doc);

    trackedSet.add("value1");
    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testRemoveNotificationOne() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.enableTracking(doc);
    trackedSet.remove("value2");
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.REMOVE, "value2", null, "value2");
    Assert.assertEquals(trackedSet.getTimeLine().getMultiValueChangeEvents().get(0), event);
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testRemoveNotificationTwo() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    doc.setPropertyInternal("tracked", trackedSet);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.remove("value2");
    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testRemoveNotificationFour() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedSet.disableTracking(doc);
    trackedSet.enableTracking(doc);
    trackedSet.remove("value5");
    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testClearOne() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<MultiValueChangeEvent<String, String>> firedEvents = new ArrayList<>();
    firedEvents.add(
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "value1", null, "value1"));
    firedEvents.add(
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "value2", null, "value2"));
    firedEvents.add(
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "value3", null, "value3"));

    trackedSet.enableTracking(doc);
    trackedSet.clear();

    Assert.assertEquals(firedEvents, trackedSet.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testClearThree() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.clear();

    Assert.assertTrue(doc.isDirty());
    db.rollback();
  }

  @Test
  public void testReturnOriginalState() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();

    final var trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");
    trackedSet.add("value4");
    trackedSet.add("value5");

    final Set<String> original = new HashSet<String>(trackedSet);
    trackedSet.enableTracking(doc);
    trackedSet.add("value6");
    trackedSet.remove("value2");
    trackedSet.remove("value5");
    trackedSet.add("value7");
    trackedSet.add("value8");
    trackedSet.remove("value7");
    trackedSet.add("value9");
    trackedSet.add("value10");

    Assert.assertEquals(
        original,
        trackedSet.returnOriginalState(db,
            (List) trackedSet.getTimeLine().getMultiValueChangeEvents()));
    db.rollback();
  }

  @Test
  public void testStackOverflowOnRecursion() {
    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    final var trackedSet = new TrackedSet<EntityImpl>(doc);
    trackedSet.add(doc);
    db.rollback();
  }
}
