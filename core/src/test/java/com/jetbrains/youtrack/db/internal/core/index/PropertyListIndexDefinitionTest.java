package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
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
 * @since 20.12.11
 */
public class PropertyListIndexDefinitionTest extends DbTestBase {

  private PropertyListIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new PropertyListIndexDefinition("testClass", "fOne", PropertyType.INTEGER);
  }

  @Test
  public void testCreateValueSingleParameter() {
    final Object result =
        propertyIndex.createValue(db, Collections.singletonList(Arrays.asList("12", "23")));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  @Test
  public void testCreateValueTwoParameters() {
    final Object result =
        propertyIndex.createValue(db, Arrays.asList(Arrays.asList("12", "23"), "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  @Test
  public void testCreateValueWrongParameter() {
    try {
      propertyIndex.createValue(db, Collections.singletonList("tt"));
      Assert.fail();
    } catch (IndexException x) {

    }
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue(db, (Object) Arrays.asList("12", "23"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue(db, Arrays.asList("12", "23"), "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    Assert.assertNull(propertyIndex.createValue(db, "tt"));
  }

  @Test
  public void testGetDocumentValueToIndex() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", Arrays.asList("12", "23"));
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(db, document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  @Test
  public void testCreateSingleValue() {
    final Object result = propertyIndex.createSingleValue(db, "12");
    Assert.assertEquals(result, 12);
  }

  @Test(expected = IndexException.class)
  public void testCreateSingleValueWrongParameter() {
    propertyIndex.createSingleValue(db, "tt");
  }

  @Test
  public void testProcessChangeEventAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);
    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddOnceWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, String> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.ADD, 0, "42");
    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

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

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 1, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 2);

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

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 1, 43);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    addedKeys.put(43, 1);

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

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveOnceWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, String> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.REMOVE, 0, null, "42");

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimes() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 1, null, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 2);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwoTimesInvValue() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.ADD, 1, 555);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    addedKeys.put(555, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemove() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);

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

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 55);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(55, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwiceRemoveOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 1, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

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

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 0, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimesAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 0, null, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.REMOVE, 1, null, 42);
    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree =
        new MultiValueChangeEvent<Integer, Integer>(ChangeType.ADD, 1, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventUpdate() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, Integer>(
            ChangeType.UPDATE, 0, 41, 42);

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(41, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventUpdateConvertValues() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<Integer, String> multiValueChangeEvent =
        new MultiValueChangeEvent<Integer, String>(
            ChangeType.UPDATE, 0, "41", "42");

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(41, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }
}
