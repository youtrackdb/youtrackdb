package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class CompositeIndexDefinitionTest extends DbTestBase {

  private CompositeIndexDefinition compositeIndex;

  @Before
  public void beforeMethod() {
    compositeIndex = new CompositeIndexDefinition("testClass");

    compositeIndex.addIndex(new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER));
    compositeIndex.addIndex(new PropertyIndexDefinition("testClass", "fTwo", PropertyType.STRING));
  }

  @Test
  public void testGetFields() {
    final List<String> fields = compositeIndex.getFields();

    Assert.assertEquals(fields.size(), 2);
    Assert.assertEquals(fields.get(0), "fOne");
    Assert.assertEquals(fields.get(1), "fTwo");
  }

  @Test
  public void testCreateValueSuccessful() {
    final Object result = compositeIndex.createValue(db, Arrays.asList("12", "test"));

    Assert.assertEquals(result, new CompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateMapValueSuccessful() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    final Object result = compositeIndexDefinition.createValue(db, 12, stringMap);

    final Collection<CompositeKey> collectionResult = (Collection<CompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new CompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new CompositeKey(12, "key2")));
  }

  @Test
  public void testCreateCollectionValueSuccessfulOne() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Arrays.asList(1, 2));

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, 1));
    expectedResult.add(new CompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateRidBagValueSuccessfulOne() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, 12, ridBag);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, new RecordId("#1:10")));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11")));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11")));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulTwo() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    final Object result =
        compositeIndexDefinition.createValue(db, Arrays.asList(Arrays.asList(1, 2), 12));

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(1, 12));
    expectedResult.add(new CompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueEmptyListOne() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, Collections.emptyList(), 12);
    Assert.assertNull(result);
  }

  @Test
  public void testCreateCollectionValueEmptyListTwo() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Collections.emptyList());
    Assert.assertNull(result);
  }

  @Test
  public void testCreateCollectionValueEmptyListOneNullSupport() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");
    compositeIndexDefinition.setNullValuesIgnored(false);

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, Collections.emptyList(), 12);
    Assert.assertEquals(result, List.of(new CompositeKey(null, 12)));
  }

  @Test
  public void testCreateCollectionValueEmptyListTwoNullSupport() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");
    compositeIndexDefinition.setNullValuesIgnored(false);

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Collections.emptyList());
    Assert.assertEquals(result, List.of(new CompositeKey(12, null)));
  }

  @Test
  public void testCreateRidBagValueSuccessfulTwo() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, Arrays.asList(ridBag, 12));

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(new RecordId("#1:10"), 12));
    expectedResult.add(new CompositeKey(new RecordId("#1:11"), 12));
    expectedResult.add(new CompositeKey(new RecordId("#1:11"), 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulThree() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.STRING));

    final Object result = compositeIndexDefinition.createValue(db, 12, Arrays.asList(1, 2), "test");

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, 1, "test"));
    expectedResult.add(new CompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateRidBagValueSuccessfulThree() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.STRING));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, 12, ridBag, "test");

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, new RecordId("#1:10"), "test"));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11"), "test"));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11"), "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expected = IndexException.class)
  public void testCreateCollectionValueTwoCollections() {
    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    compositeIndexDefinition.createValue(db, Arrays.asList(1, 2), List.of(12));
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParam() {
    compositeIndex.createValue(db, Arrays.asList("1t2", "test"));
  }

  @Test
  public void testCreateValueSuccessfulArrayParams() {
    final Object result = compositeIndex.createValue(db, "12", "test");

    Assert.assertEquals(result, new CompositeKey(Arrays.asList(12, "test")));
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParamArrayParams() {
    compositeIndex.createValue(db, "1t2", "test");
  }

  @Test
  public void testCreateValueDefinitionsMoreThanParams() {
    compositeIndex.addIndex(
        new PropertyIndexDefinition("testClass", "fThree", PropertyType.STRING));

    final Object result = compositeIndex.createValue(db, "12", "test");
    Assert.assertEquals(result, new CompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateValueIndexItemWithTwoParams() {
    final CompositeIndexDefinition anotherCompositeIndex =
        new CompositeIndexDefinition("testClass");

    anotherCompositeIndex.addIndex(
        new PropertyIndexDefinition("testClass", "f11", PropertyType.STRING));
    anotherCompositeIndex.addIndex(
        new PropertyIndexDefinition("testClass", "f22", PropertyType.STRING));

    compositeIndex.addIndex(anotherCompositeIndex);

    final Object result = compositeIndex.createValue(db, "12", "test", "tset");
    Assert.assertEquals(result, new CompositeKey(Arrays.asList(12, "test", "tset")));
  }

  @Test
  public void testDocumentToIndexSuccessful() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", "test");

    final Object result = compositeIndex.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, new CompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testDocumentToIndexMapValueSuccessful() {
    final EntityImpl document = new EntityImpl();

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    document.field("fOne", 12);
    document.field("fTwo", stringMap);

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    final Collection<CompositeKey> collectionResult = (Collection<CompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new CompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new CompositeKey(12, "key2")));
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulOne() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, 1));
    expectedResult.add(new CompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyOne() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Collections.emptyList());

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertNull(result);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyTwo() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", Collections.emptyList());
    document.field("fTwo", 12);

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertNull(result);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyOneNullValuesSupport() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Collections.emptyList());

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.setNullValuesIgnored(false);

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, List.of(new CompositeKey(12, null)));
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyTwoNullValuesSupport() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", Collections.emptyList());
    document.field("fTwo", 12);

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.setNullValuesIgnored(false);

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, List.of(new CompositeKey(null, 12)));
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulOne() {
    final EntityImpl document = new EntityImpl();

    final RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    document.field("fOne", 12);
    document.field("fTwo", ridBag);

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, new RecordId("#1:10")));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11")));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11")));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulTwo() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(1, 12));
    expectedResult.add(new CompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulTwo() {
    final RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", ridBag);

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(new RecordId("#1:10"), 12));
    expectedResult.add(new CompositeKey(new RecordId("#1:11"), 12));
    expectedResult.add(new CompositeKey(new RecordId("#1:11"), 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulThree() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));
    document.field("fThree", "test");

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.STRING));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, 1, "test"));
    expectedResult.add(new CompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulThree() {
    final EntityImpl document = new EntityImpl();

    final RidBag ridBag = new RidBag(db);
    ridBag.add(new RecordId("#1:10"));
    ridBag.add(new RecordId("#1:11"));
    ridBag.add(new RecordId("#1:11"));

    document.field("fOne", 12);
    document.field("fTwo", ridBag);
    document.field("fThree", "test");

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.STRING));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<CompositeKey> expectedResult = new ArrayList<CompositeKey>();

    expectedResult.add(new CompositeKey(12, new RecordId("#1:10"), "test"));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11"), "test"));
    expectedResult.add(new CompositeKey(12, new RecordId("#1:11"), "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expected = BaseException.class)
  public void testDocumentToIndexCollectionValueTwoCollections() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", List.of(12));
    document.field("fTwo", Arrays.asList(1, 2));

    final CompositeIndexDefinition compositeIndexDefinition =
        new CompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.INTEGER));
    compositeIndexDefinition.getDocumentValueToIndex(db, document);
  }

  @Test(expected = DatabaseException.class)
  public void testDocumentToIndexWrongField() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", "1t2");
    document.field("fTwo", "test");

    compositeIndex.getDocumentValueToIndex(db, document);
  }

  @Test
  public void testGetParamCount() {
    final int result = compositeIndex.getParamCount();

    Assert.assertEquals(result, 2);
  }

  @Test
  public void testGetTypes() {
    final PropertyType[] result = compositeIndex.getTypes();

    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], PropertyType.INTEGER);
    Assert.assertEquals(result[1], PropertyType.STRING);
  }

  @Test
  public void testEmptyIndexReload() {
    final DatabaseDocumentTx database = new DatabaseDocumentTx("memory:compositetestone");
    database.create();

    final CompositeIndexDefinition emptyCompositeIndex =
        new CompositeIndexDefinition("testClass");

    emptyCompositeIndex.addIndex(
        new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER));
    emptyCompositeIndex.addIndex(
        new PropertyIndexDefinition("testClass", "fTwo", PropertyType.STRING));

    database.begin();
    final EntityImpl docToStore = emptyCompositeIndex.toStream(new EntityImpl());
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final EntityImpl docToLoad = database.load(docToStore.getIdentity());

    final CompositeIndexDefinition result = new CompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndex);
  }

  @Test
  public void testIndexReload() {
    final EntityImpl docToStore = compositeIndex.toStream(new EntityImpl());

    final CompositeIndexDefinition result = new CompositeIndexDefinition();
    result.fromStream(docToStore);

    Assert.assertEquals(result, compositeIndex);
  }

  @Test
  public void testClassOnlyConstructor() {
    final DatabaseDocumentTx database = new DatabaseDocumentTx("memory:compositetesttwo");
    database.create();

    final CompositeIndexDefinition emptyCompositeIndex =
        new CompositeIndexDefinition(
            "testClass",
            Arrays.asList(
                new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER),
                new PropertyIndexDefinition("testClass", "fTwo", PropertyType.STRING)));

    final CompositeIndexDefinition emptyCompositeIndexTwo =
        new CompositeIndexDefinition("testClass");

    emptyCompositeIndexTwo.addIndex(
        new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER));
    emptyCompositeIndexTwo.addIndex(
        new PropertyIndexDefinition("testClass", "fTwo", PropertyType.STRING));

    Assert.assertEquals(emptyCompositeIndex, emptyCompositeIndexTwo);

    database.begin();
    final EntityImpl docToStore = emptyCompositeIndex.toStream(new EntityImpl());
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final EntityImpl docToLoad = database.load(docToStore.getIdentity());

    final CompositeIndexDefinition result = new CompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndexTwo);
  }

  @Test
  public void testProcessChangeListEventsOne() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.STRING));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedList<String> trackedList = new TrackedList<String>(doc);
    trackedList.enableTracking(doc);
    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedList.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l3", 3)));
  }

  @Test
  public void testProcessChangeRidBagEventsOne() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final RidBag ridBag = new RidBag(db);
    ridBag.enableTracking(null);
    ridBag.add(new RecordId("#10:0"));
    ridBag.add(new RecordId("#10:1"));
    ridBag.add(new RecordId("#10:0"));
    ridBag.add(new RecordId("#10:2"));
    ridBag.remove(new RecordId("#10:0"));
    ridBag.remove(new RecordId("#10:1"));

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        ridBag.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, new RecordId("#10:0"), 3)));
    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, new RecordId("#10:2"), 3)));
  }

  @Test
  public void testProcessChangeListEventsTwo() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.STRING));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedList<String> trackedList = new TrackedList<String>(doc);

    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    trackedList.enableTracking(doc);
    trackedList.add("l4");
    trackedList.remove("l1");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedList.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new CompositeKey(2, "l1", 3)));
  }

  @Test
  public void testProcessChangeRidBagEventsTwo() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final RidBag ridBag = new RidBag(db);

    ridBag.add(new RecordId("#10:1"));
    ridBag.add(new RecordId("#10:2"));
    ridBag.add(new RecordId("#10:3"));
    ridBag.remove(new RecordId("#10:2"));
    ridBag.disableTracking(null);
    ridBag.enableTracking(null);

    ridBag.add(new RecordId("#10:4"));
    ridBag.remove(new RecordId("#10:1"));

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        ridBag.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, new RecordId("#10:4"), 3)));
    Assert.assertTrue(keysToRemove.containsKey(new CompositeKey(2, new RecordId("#10:1"), 3)));
  }

  @Test
  public void testProcessChangeSetEventsOne() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.STRING));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);

    trackedSet.enableTracking(doc);
    trackedSet.add("l1");
    trackedSet.add("l2");
    trackedSet.add("l3");
    trackedSet.remove("l2");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedSet.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l3", 3)));
  }

  @Test
  public void testProcessChangeSetEventsTwo() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyListIndexDefinition("testCollectionClass", "fTwo", PropertyType.STRING));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedMap = new TrackedSet<String>(doc);

    trackedMap.add("l1");
    trackedMap.add("l2");
    trackedMap.add("l3");
    trackedMap.remove("l2");

    trackedMap.enableTracking(doc);
    trackedMap.add("l4");
    trackedMap.remove("l1");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new CompositeKey(2, "l1", 3)));
  }

  @Test
  public void testProcessChangeKeyMapEventsOne() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);
    trackedMap.enableTracking(doc);
    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "k1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "k3", 3)));
  }

  @Test
  public void testProcessChangeKeyMapEventsTwo() {
    final CompositeIndexDefinition compositeIndexDefinition = new CompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fOne", PropertyType.INTEGER));
    compositeIndexDefinition.addIndex(
        new PropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(
        new PropertyIndexDefinition("testCollectionClass", "fThree", PropertyType.INTEGER));

    final EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);

    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");
    trackedMap.enableTracking(doc);

    trackedMap.put("k4", "v4");
    trackedMap.remove("k1");

    Object2IntOpenHashMap<CompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<CompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (MultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new CompositeKey(2, "k4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new CompositeKey(2, "k1", 3)));
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", compositeIndex.getClassName());
  }
}
