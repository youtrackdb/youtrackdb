package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.OMemoryStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TrackedMapTest extends DBTestBase {

  @Test
  public void testPutOne() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.enableTracking(doc);

    map.put("key1", "value1");

    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.ADD, "key1", "value1", null);
    Assert.assertEquals(event, map.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(map.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testPutTwo() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    map.put("key1", "value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.disableTracking(doc);
    map.enableTracking(doc);

    map.put("key1", "value2");
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.UPDATE, "key1", "value2", "value1");
    Assert.assertEquals(event, map.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(map.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testPutThree() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    map.put("key1", "value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.disableTracking(doc);
    map.enableTracking(doc);
    map.put("key1", "value1");

    Assert.assertFalse(map.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testPutFour() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    map.put("key1", "value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.disableTracking(doc);
    map.enableTracking(doc);

    map.put("key1", "value1");

    Assert.assertFalse(map.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testPutFive() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.enableTracking(doc);

    map.putInternal("key1", "value1");

    Assert.assertFalse(map.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testRemoveOne() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);

    map.put("key1", "value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.disableTracking(doc);
    map.enableTracking(doc);

    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "key1", null, "value1");
    map.remove("key1");
    Assert.assertEquals(event, map.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(map.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveTwo() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> map = new TrackedMap<String>(doc);

    map.put("key1", "value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    map.disableTracking(doc);
    map.enableTracking(doc);

    map.remove("key2");

    Assert.assertFalse(map.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testClearOne() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);

    trackedMap.put("key1", "value1");
    trackedMap.put("key2", "value2");
    trackedMap.put("key3", "value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<OMultiValueChangeEvent<Object, String>> firedEvents = new ArrayList<>();
    firedEvents.add(
        new OMultiValueChangeEvent<Object, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "key1", null, "value1"));
    firedEvents.add(
        new OMultiValueChangeEvent<Object, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "key2", null, "value2"));
    firedEvents.add(
        new OMultiValueChangeEvent<Object, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "key3", null, "value3"));

    trackedMap.enableTracking(doc);
    trackedMap.clear();

    Assert.assertEquals(trackedMap.getTimeLine().getMultiValueChangeEvents(), firedEvents);
    Assert.assertTrue(trackedMap.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearThree() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);

    trackedMap.put("key1", "value1");
    trackedMap.put("key2", "value2");
    trackedMap.put("key3", "value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedMap.clear();

    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testReturnOriginalStateOne() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);
    trackedMap.put("key1", "value1");
    trackedMap.put("key2", "value2");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.put("key5", "value5");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value7");

    final Map<Object, String> original = new HashMap<Object, String>(trackedMap);
    trackedMap.enableTracking(doc);
    trackedMap.put("key8", "value8");
    trackedMap.put("key9", "value9");
    trackedMap.put("key2", "value10");
    trackedMap.put("key11", "value11");
    trackedMap.remove("key5");
    trackedMap.remove("key5");
    trackedMap.put("key3", "value12");
    trackedMap.remove("key8");
    trackedMap.remove("key3");

    Assert.assertEquals(
        trackedMap.returnOriginalState(db,
            (List) trackedMap.getTimeLine().getMultiValueChangeEvents()),
        original);
  }

  @Test
  public void testReturnOriginalStateTwo() {
    final EntityImpl doc = new EntityImpl();

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);
    trackedMap.put("key1", "value1");
    trackedMap.put("key2", "value2");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.put("key5", "value5");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value7");

    final Map<Object, String> original = new HashMap<Object, String>(trackedMap);
    trackedMap.enableTracking(doc);
    trackedMap.put("key8", "value8");
    trackedMap.put("key9", "value9");
    trackedMap.put("key2", "value10");
    trackedMap.put("key11", "value11");
    trackedMap.remove("key5");
    trackedMap.remove("key5");
    trackedMap.clear();
    trackedMap.put("key3", "value12");
    trackedMap.remove("key8");
    trackedMap.remove("key3");

    Assert.assertEquals(
        trackedMap.returnOriginalState(db,
            (List) trackedMap.getTimeLine().getMultiValueChangeEvents()),
        original);
  }

  /**
   * Test that {@link TrackedMap} is serialised correctly.
   */
  @Test
  public void testMapSerialization() throws Exception {

    class NotSerializableEntityImpl extends EntityImpl {

      private static final long serialVersionUID = 1L;

      private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
      }
    }

    final TrackedMap<String> beforeSerialization =
        new TrackedMap<String>(new NotSerializableEntityImpl());
    beforeSerialization.put(0, "firstVal");
    beforeSerialization.put(1, "secondVal");

    final OMemoryStream memoryStream = new OMemoryStream();
    final ObjectOutputStream out = new ObjectOutputStream(memoryStream);
    out.writeObject(beforeSerialization);
    out.close();

    final ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(memoryStream.copy()));
    @SuppressWarnings("unchecked") final Map<Object, String> afterSerialization = (Map<Object, String>) input.readObject();

    Assert.assertEquals(afterSerialization.size(), beforeSerialization.size());
    for (int i = 0; i < afterSerialization.size(); i++) {
      Assert.assertEquals(afterSerialization.get(i), beforeSerialization.get(i));
    }
  }
}
