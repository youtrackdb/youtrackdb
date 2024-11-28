package com.orientechnologies.orient.core.index;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
public abstract class OPropertyRidBagAbstractIndexDefinition extends BaseMemoryDatabase {

  private OPropertyRidBagIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new OPropertyRidBagIndexDefinition("testClass", "fOne");
  }

  @Test
  public void testCreateValueSingleParameter() {
    ORidBag ridBag = new ORidBag(db);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, Collections.singletonList(ridBag));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueTwoParameters() {
    ORidBag ridBag = new ORidBag(db);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, Arrays.asList(ridBag, "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueWrongParameter() {
    Assert.assertNull(propertyIndex.createValue(db, Collections.singletonList("tt")));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    ORidBag ridBag = new ORidBag(db);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, ridBag);

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));

    assertEmbedded(ridBag);
  }


  @Test
  public void testCreateValueTwoParametersArrayParams() {
    ORidBag ridBag = new ORidBag(db);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(db, ridBag, "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    Assert.assertNull(propertyIndex.createValue(db, "tt"));
  }

  @Test
  public void testGetDocumentValueToIndex() {
    ORidBag ridBag = new ORidBag(db);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final ODocument document = new ODocument();

    document.field("fOne", ridBag);
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(db, document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(2, collectionResult.size());

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));

    assertEmbedded(ridBag);
  }

  @Test
  public void testProcessChangeEventAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEvent =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);

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

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 2);

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

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:13"), new ORecordId("#1:13"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);
    addedKeys.put(new ORecordId("#1:13"), 1);

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

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEvent =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimes() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 2);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemove() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

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

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:13"),
            null,
            new ORecordId("#1:13"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:13"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwiceRemoveOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);

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

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimesAddOnce() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(db, multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(db, multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  abstract void assertEmbedded(ORidBag ridBag);
}
