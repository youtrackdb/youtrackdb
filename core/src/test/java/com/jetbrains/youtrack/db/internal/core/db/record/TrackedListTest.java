package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
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
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TrackedListTest extends DbTestBase {

  @Test
  public void testAddNotificationOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.enableTracking(doc);
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.ADD, 0, "value1", null);
    trackedList.add("value1");

    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.ADD, 2, "value3", null);

    trackedList.add("value3");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationThree() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationFour() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);
    Assert.assertFalse(trackedList.isModified());
    trackedList.enableTracking(doc);

    trackedList.addInternal("value3");
    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<MultiValueChangeEvent<Integer, String>> firedEvents =
        new ArrayList<MultiValueChangeEvent<Integer, String>>();
    firedEvents.add(
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.ADD, 0, "value1"));
    firedEvents.add(
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.ADD, 1, "value3"));
    trackedList.enableTracking(doc);
    trackedList.addAll(valuesToAdd);

    Assert.assertEquals(firedEvents, trackedList.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    trackedList.addAll(valuesToAdd);

    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationThree() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);
    for (var e : valuesToAdd) {
      trackedList.addInternal(e);
    }

    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddIndexNotificationOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);

    var event =
        new MultiValueChangeEvent<Integer, String>(ChangeType.ADD, 1, "value3", null);

    trackedList.add(1, "value3");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddIndexNotificationTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("aa", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.add(1, "value3");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testSetNotificationOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.enableTracking(doc);
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.UPDATE, 1, "value4", "value2");
    trackedList.set(1, "value4");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testSetNotificationTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.set(1, "value4");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.enableTracking(doc);
    trackedList.remove("value2");
    var event =
        new MultiValueChangeEvent<Integer, String>(ChangeType.REMOVE, 1, null, "value2");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.remove("value2");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationFour() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);

    trackedList.remove("value4");
    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testRemoveIndexOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.enableTracking(doc);

    trackedList.remove(1);
    var event =
        new MultiValueChangeEvent<Object, Object>(
            ChangeType.REMOVE, 1, null, "value2");
    Assert.assertTrue(trackedList.isModified());
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearOne() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<MultiValueChangeEvent<Integer, String>> firedEvents =
        new ArrayList<MultiValueChangeEvent<Integer, String>>();
    firedEvents.add(
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.REMOVE, 2, null, "value3"));
    firedEvents.add(
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.REMOVE, 1, null, "value2"));
    firedEvents.add(
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.REMOVE, 0, null, "value1"));
    trackedList.enableTracking(doc);

    trackedList.clear();
    Assert.assertEquals(firedEvents, trackedList.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearTwo() {
    final var doc = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final var trackedList = new TrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.clear();
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testReturnOriginalStateOne() {
    final var doc = (EntityImpl) db.newEntity();

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");
    trackedList.add("value4");
    trackedList.add("value5");

    final List<String> original = new ArrayList<String>(trackedList);
    trackedList.enableTracking(doc);
    trackedList.add("value6");
    trackedList.add("value7");
    trackedList.set(2, "value10");
    trackedList.add(1, "value8");
    trackedList.add(1, "value8");
    trackedList.remove(3);
    trackedList.remove("value7");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.remove("value9");
    trackedList.remove("value9");
    trackedList.add(4, "value11");

    Assert.assertEquals(
        original,
        trackedList.returnOriginalState(db,
            (List) trackedList.getTimeLine().getMultiValueChangeEvents()));
  }

  @Test
  public void testReturnOriginalStateTwo() {
    final var doc = (EntityImpl) db.newEntity();

    final var trackedList = new TrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");
    trackedList.add("value4");
    trackedList.add("value5");

    final List<String> original = new ArrayList<String>(trackedList);
    trackedList.enableTracking(doc);
    trackedList.add("value6");
    trackedList.add("value7");
    trackedList.set(2, "value10");
    trackedList.add(1, "value8");
    trackedList.remove(3);
    trackedList.clear();
    trackedList.remove("value7");
    trackedList.add(0, "value9");
    trackedList.add("value11");
    trackedList.add(0, "value12");
    trackedList.add("value12");

    Assert.assertEquals(
        original,
        trackedList.returnOriginalState(db,
            (List) trackedList.getTimeLine().getMultiValueChangeEvents()));
  }

  /**
   * Test that {@link TrackedList} is serialised correctly.
   */
  @Test
  public void testSerialization() throws Exception {

    class NotSerializableEntityImpl extends EntityImpl {

      private static final long serialVersionUID = 1L;

      public NotSerializableEntityImpl(
          DatabaseSessionInternal database) {
        super(database);
      }

      private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
      }
    }

    final var beforeSerialization =
        new TrackedList<String>(new NotSerializableEntityImpl(db));
    beforeSerialization.add("firstVal");
    beforeSerialization.add("secondVal");

    final var memoryStream = new MemoryStream();
    var out = new ObjectOutputStream(memoryStream);
    out.writeObject(beforeSerialization);
    out.close();

    final var input =
        new ObjectInputStream(new ByteArrayInputStream(memoryStream.copy()));
    @SuppressWarnings("unchecked") final var afterSerialization = (List<String>) input.readObject();

    Assert.assertEquals(afterSerialization.size(), beforeSerialization.size());
    for (var i = 0; i < afterSerialization.size(); i++) {
      Assert.assertEquals(afterSerialization.get(i), beforeSerialization.get(i));
    }
  }
}
