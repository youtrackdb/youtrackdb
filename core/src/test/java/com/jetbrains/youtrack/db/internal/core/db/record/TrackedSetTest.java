package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.MemoryStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TrackedSetTest extends DbTestBase {

  @Test
  public void testAddOne() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    MultiValueChangeEvent<Object, Object> event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.ADD, "value1", "value1", null);
    trackedSet.add("value1");
    Assert.assertEquals(event, trackedSet.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddTwo() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    doc.setProperty("tracked", trackedSet);
    trackedSet.add("value1");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddThree() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    trackedSet.addInternal("value1");

    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddFour() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);

    trackedSet.add("value1");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.disableTracking(doc);
    trackedSet.enableTracking(doc);

    trackedSet.add("value1");
    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationOne() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.enableTracking(doc);
    trackedSet.remove("value2");
    MultiValueChangeEvent<Object, Object> event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.REMOVE, "value2", null, "value2");
    Assert.assertEquals(trackedSet.getTimeLine().getMultiValueChangeEvents().get(0), event);
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationTwo() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    doc.setProperty("tracked", trackedSet);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.remove("value2");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationFour() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
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
  }

  @Test
  public void testClearOne() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
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
  }

  @Test
  public void testClearThree() {
    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.clear();

    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testReturnOriginalState() {
    final EntityImpl doc = new EntityImpl();

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);
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
  }

  /**
   * Test that {@link TrackedSet} is serialised correctly.
   */
  @Test
  public void testSetSerialization() throws Exception {

    class NotSerializableEntityImpl extends EntityImpl {

      private static final long serialVersionUID = 1L;

      private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
      }
    }

    final TrackedSet<String> beforeSerialization =
        new TrackedSet<String>(new NotSerializableEntityImpl());
    beforeSerialization.add("firstVal");
    beforeSerialization.add("secondVal");

    final MemoryStream memoryStream = new MemoryStream();
    ObjectOutputStream out = new ObjectOutputStream(memoryStream);
    out.writeObject(beforeSerialization);
    out.close();

    final ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(memoryStream.copy()));
    @SuppressWarnings("unchecked") final Set<String> afterSerialization = (Set<String>) input.readObject();

    Assert.assertEquals(afterSerialization.size(), beforeSerialization.size());
    Assert.assertTrue(beforeSerialization.containsAll(afterSerialization));
  }

  @Test
  public void testStackOverflowOnRecursion() {
    final EntityImpl doc = new EntityImpl();
    final TrackedSet<EntityImpl> trackedSet = new TrackedSet<>(doc);
    trackedSet.add(doc);
  }
}
