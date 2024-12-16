package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 20.12.11
 */
public class PropertyMapIndexDefinitionTest extends DbTestBase {

  private final Map<String, Integer> mapToTest = new HashMap<String, Integer>();
  private PropertyMapIndexDefinition propertyIndexByKey;
  private PropertyMapIndexDefinition propertyIndexByValue;
  private PropertyMapIndexDefinition propertyIndexByIntegerKey;

  @Before
  public void beforeClass() {
    mapToTest.put("st1", 1);
    mapToTest.put("st2", 2);
  }

  @Before
  public void beforeMethod() {
    propertyIndexByKey =
        new PropertyMapIndexDefinition(
            "testClass", "fOne", PropertyType.STRING, PropertyMapIndexDefinition.INDEX_BY.KEY);
    propertyIndexByIntegerKey =
        new PropertyMapIndexDefinition(
            "testClass", "fTwo", PropertyType.INTEGER, PropertyMapIndexDefinition.INDEX_BY.KEY);
    propertyIndexByValue =
        new PropertyMapIndexDefinition(
            "testClass", "fOne", PropertyType.INTEGER, PropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateValueByKeySingleParameter() {
    final Object result = propertyIndexByKey.createValue(db, Collections.singletonList(mapToTest));
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains("st1"));
    Assert.assertTrue(collectionResult.contains("st2"));
  }

  @Test
  public void testCreateValueByValueSingleParameter() {
    final Object result =
        propertyIndexByValue.createValue(db, Collections.singletonList(mapToTest));
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains(1));
    Assert.assertTrue(collectionResult.contains(2));
  }

  @Test
  public void testCreateValueByKeyTwoParameters() {
    final Object result = propertyIndexByKey.createValue(db, Arrays.asList(mapToTest, "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains("st1"));
    Assert.assertTrue(collectionResult.contains("st2"));
  }

  @Test
  public void testCreateValueByValueTwoParameters() {
    final Object result = propertyIndexByValue.createValue(db, Arrays.asList(mapToTest, "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains(1));
    Assert.assertTrue(collectionResult.contains(2));
  }

  @Test
  public void testCreateValueWrongParameter() {
    final Object result = propertyIndexByKey.createValue(db, Collections.singletonList("tt"));
    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueByKeySingleParameterArrayParams() {
    final Object result = propertyIndexByKey.createValue(db, mapToTest);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains("st1"));
    Assert.assertTrue(collectionResult.contains("st2"));
  }

  @Test
  public void testCreateValueByValueSingleParameterArrayParams() {
    final Object result = propertyIndexByValue.createValue(db, mapToTest);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains(1));
    Assert.assertTrue(collectionResult.contains(2));
  }

  @Test
  public void testCreateValueByKeyTwoParametersArrayParams() {
    final Object result = propertyIndexByKey.createValue(db, mapToTest, "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains("st1"));
    Assert.assertTrue(collectionResult.contains("st2"));
  }

  @Test
  public void testCreateValueByValueTwoParametersArrayParams() {
    final Object result = propertyIndexByValue.createValue(db, mapToTest, "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains(1));
    Assert.assertTrue(collectionResult.contains(2));
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    final Object result = propertyIndexByKey.createValue(db, "tt");
    Assert.assertNull(result);
  }

  @Test
  public void testGetDocumentValueByKeyToIndex() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", mapToTest);
    document.field("fTwo", 10);

    final Object result = propertyIndexByKey.getDocumentValueToIndex(db, document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains("st1"));
    Assert.assertTrue(collectionResult.contains("st2"));
  }

  @Test
  public void testGetDocumentValueByValueToIndex() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", mapToTest);
    document.field("fTwo", 10);

    final Object result = propertyIndexByValue.getDocumentValueToIndex(db, document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;

    Assert.assertEquals(2, collectionResult.size());
    Assert.assertTrue(collectionResult.contains(1));
    Assert.assertTrue(collectionResult.contains(2));
  }

  @Test
  public void testGetFields() {
    final List<String> result = propertyIndexByKey.getFields();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("fOne", result.getFirst());
  }

  @Test
  public void testGetTypes() {
    final PropertyType[] result = propertyIndexByKey.getTypes();
    Assert.assertEquals(1, result.length);
    Assert.assertEquals(PropertyType.STRING, result[0]);
  }

  @Test
  public void testEmptyIndexByKeyReload() {
    propertyIndexByKey =
        new PropertyMapIndexDefinition(
            "tesClass", "fOne", PropertyType.STRING, PropertyMapIndexDefinition.INDEX_BY.KEY);

    db.begin();
    final EntityImpl docToStore = propertyIndexByKey.toStream(new EntityImpl());
    db.save(docToStore);
    db.commit();

    final EntityImpl docToLoad = db.load(docToStore.getIdentity());

    final PropertyIndexDefinition result = new PropertyMapIndexDefinition();
    result.fromStream(docToLoad);
    Assert.assertEquals(result, propertyIndexByKey);
  }

  @Test
  public void testEmptyIndexByValueReload() {
    propertyIndexByValue =
        new PropertyMapIndexDefinition(
            "tesClass", "fOne", PropertyType.INTEGER, PropertyMapIndexDefinition.INDEX_BY.VALUE);

    db.begin();
    final EntityImpl docToStore = propertyIndexByValue.toStream(new EntityImpl());
    db.save(docToStore);
    db.commit();

    final EntityImpl docToLoad = db.load(docToStore.getIdentity());

    final PropertyIndexDefinition result = new PropertyMapIndexDefinition();
    result.fromStream(docToLoad);

    Assert.assertEquals(result, propertyIndexByValue);
  }

  @Test
  public void testCreateSingleValueByKey() {
    final Object result = propertyIndexByKey.createSingleValue(db, "tt");
    Assert.assertEquals("tt", result);
  }

  @Test
  public void testCreateSingleValueByValue() {
    final Object result = propertyIndexByValue.createSingleValue(db, "12");
    Assert.assertEquals(12, result);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateWrongSingleValueByValue() {
    propertyIndexByValue.createSingleValue(db, "tt");
  }

  @Test(expected = NullPointerException.class)
  public void testIndexByIsRequired() {
    new PropertyMapIndexDefinition("testClass", "testField", PropertyType.STRING, null);
  }

  @Test
  public void testCreateDDLByKey() {
    final String ddl =
        propertyIndexByKey
            .toCreateIndexDDL("testIndex", "unique", null)
            .toLowerCase(Locale.ENGLISH);
    Assert.assertEquals("create index `testindex` on `testclass` ( `fone` by key ) unique", ddl);
  }

  @Test
  public void testCreateDDLByValue() {
    final String ddl =
        propertyIndexByValue
            .toCreateIndexDDL("testIndex", "unique", null)
            .toLowerCase(Locale.ENGLISH);
    Assert.assertEquals("create index `testindex` on `testclass` ( `fone` by value ) unique", ddl);
  }

  @Test
  public void testProcessChangeEventAddKey() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.ADD, "key1", "value1");

    propertyIndexByKey.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put("key1", 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddKeyWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.ADD, "12", "value1");

    propertyIndexByIntegerKey.processChangeEvent(
        db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(12, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddValue() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<String, Integer>(
            ChangeType.ADD, "key1", 42);

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddValueWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.ADD, "12", "42");

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveKey() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "key1", "value1");

    propertyIndexByKey.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put("key1", 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveKeyWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "12", "value1");

    propertyIndexByIntegerKey.processChangeEvent(
        db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(12, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveValue() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<String, Integer>(
            ChangeType.REMOVE, "key1", null, 42);

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveValueWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.REMOVE, "12", null, "42");

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventUpdateKey() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<String, Integer>(
            ChangeType.UPDATE, "key1", 42);

    propertyIndexByKey.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);
    Assert.assertTrue(keysToAdd.isEmpty());
    Assert.assertTrue(keysToRemove.isEmpty());
  }

  @Test
  public void testProcessChangeEventUpdateValue() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, Integer> multiValueChangeEvent =
        new MultiValueChangeEvent<String, Integer>(
            ChangeType.UPDATE, "key1", 41, 42);

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(41, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventUpdateValueWithConversion() {
    final Object2IntOpenHashMap<Object> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    final Object2IntOpenHashMap<Object> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    final MultiValueChangeEvent<String, String> multiValueChangeEvent =
        new MultiValueChangeEvent<String, String>(
            ChangeType.UPDATE, "12", "42", "41");

    propertyIndexByValue.processChangeEvent(db, multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(41, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }
}
