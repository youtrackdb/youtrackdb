package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 1/30/14
 */
public abstract class PropertyRidBagAbstractIndexDefinition extends DbTestBase {

  private PropertyRidBagIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new PropertyRidBagIndexDefinition("testClass", "fOne");
  }

  @Test
  public void testCreateValueSingleParameter() {
    RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#1:12"));
    ridBag.add(new RecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, Collections.singletonList(ridBag));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new RecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new RecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueTwoParameters() {
    RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#1:12"));
    ridBag.add(new RecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, Arrays.asList(ridBag, "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new RecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new RecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueWrongParameter() {
    Assert.assertNull(propertyIndex.createValue(db, Collections.singletonList("tt")));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#1:12"));
    ridBag.add(new RecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, ridBag);

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new RecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new RecordId("#1:23")));

    assertEmbedded(ridBag);
  }


  @Test
  public void testCreateValueTwoParametersArrayParams() {
    RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#1:12"));
    ridBag.add(new RecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, ridBag, "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new RecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new RecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    Assert.assertNull(propertyIndex.createValue(db, "tt"));
  }

  @Test
  public void testGetDocumentValueToIndex() {
    RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#1:12"));
    ridBag.add(new RecordId("#1:23"));

    final EntityImpl document = new EntityImpl();

    document.field("fOne", ridBag);
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(db, document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new RecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new RecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testProcessChangeEventAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEvent =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new RecordId("#1:12"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwoTimes() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new RecordId("#1:12"), 2);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwoValues() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:13"),
            new RecordId("#1:13"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new RecordId("#1:12"), 1);
    addedKeys.put(new RecordId("#1:13"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEvent =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new RecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimes() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new RecordId("#1:12"), 2);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemove() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemoveInvValue() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:13"),
            null,
            new RecordId("#1:13"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new RecordId("#1:12"), 1);
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new RecordId("#1:13"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwiceRemoveOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventThree =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new RecordId("#1:12"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddOnceRemoveTwice() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventThree =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new RecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimesAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventOne =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.REMOVE,
            new RecordId("#1:12"),
            null,
            new RecordId("#1:12"));
    final MultiValueChangeEvent<Identifiable, Identifiable> multiValueChangeEventThree =
        new MultiValueChangeEvent<Identifiable, Identifiable>(
            ChangeType.ADD, new RecordId("#1:12"),
            new RecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new RecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  abstract void assertEmbedded(RidBag ridBag);
}
