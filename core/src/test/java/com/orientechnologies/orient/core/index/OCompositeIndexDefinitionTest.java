package com.orientechnologies.orient.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.TrackedList;
import com.orientechnologies.orient.core.db.record.TrackedMap;
import com.orientechnologies.orient.core.db.record.TrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.RidBag;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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
public class OCompositeIndexDefinitionTest extends DBTestBase {

  private OCompositeIndexDefinition compositeIndex;

  @Before
  public void beforeMethod() {
    compositeIndex = new OCompositeIndexDefinition("testClass");

    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fOne", YTType.INTEGER));
    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fTwo", YTType.STRING));
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

    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateMapValueSuccessful() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    final Object result = compositeIndexDefinition.createValue(db, 12, stringMap);

    final Collection<OCompositeKey> collectionResult = (Collection<OCompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key2")));
  }

  @Test
  public void testCreateCollectionValueSuccessfulOne() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Arrays.asList(1, 2));

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1));
    expectedResult.add(new OCompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateRidBagValueSuccessfulOne() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, 12, ridBag);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:10")));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11")));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11")));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    final Object result =
        compositeIndexDefinition.createValue(db, Arrays.asList(Arrays.asList(1, 2), 12));

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(1, 12));
    expectedResult.add(new OCompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueEmptyListOne() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, Collections.emptyList(), 12);
    Assert.assertNull(result);
  }

  @Test
  public void testCreateCollectionValueEmptyListTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Collections.emptyList());
    Assert.assertNull(result);
  }

  @Test
  public void testCreateCollectionValueEmptyListOneNullSupport() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");
    compositeIndexDefinition.setNullValuesIgnored(false);

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, Collections.emptyList(), 12);
    Assert.assertEquals(result, List.of(new OCompositeKey(null, 12)));
  }

  @Test
  public void testCreateCollectionValueEmptyListTwoNullSupport() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");
    compositeIndexDefinition.setNullValuesIgnored(false);

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(db, 12, Collections.emptyList());
    Assert.assertEquals(result, List.of(new OCompositeKey(12, null)));
  }

  @Test
  public void testCreateRidBagValueSuccessfulTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, Arrays.asList(ridBag, 12));

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(new YTRecordId("#1:10"), 12));
    expectedResult.add(new OCompositeKey(new YTRecordId("#1:11"), 12));
    expectedResult.add(new OCompositeKey(new YTRecordId("#1:11"), 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulThree() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.STRING));

    final Object result = compositeIndexDefinition.createValue(db, 12, Arrays.asList(1, 2), "test");

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1, "test"));
    expectedResult.add(new OCompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateRidBagValueSuccessfulThree() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.STRING));

    RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    final Object result = compositeIndexDefinition.createValue(db, 12, ridBag, "test");

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:10"), "test"));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11"), "test"));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11"), "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expected = YTIndexException.class)
  public void testCreateCollectionValueTwoCollections() {
    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    compositeIndexDefinition.createValue(db, Arrays.asList(1, 2), List.of(12));
  }

  @Test(expected = YTDatabaseException.class)
  public void testCreateValueWrongParam() {
    compositeIndex.createValue(db, Arrays.asList("1t2", "test"));
  }

  @Test
  public void testCreateValueSuccessfulArrayParams() {
    final Object result = compositeIndex.createValue(db, "12", "test");

    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test(expected = YTDatabaseException.class)
  public void testCreateValueWrongParamArrayParams() {
    compositeIndex.createValue(db, "1t2", "test");
  }

  @Test
  public void testCreateValueDefinitionsMoreThanParams() {
    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fThree", YTType.STRING));

    final Object result = compositeIndex.createValue(db, "12", "test");
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateValueIndexItemWithTwoParams() {
    final OCompositeIndexDefinition anotherCompositeIndex =
        new OCompositeIndexDefinition("testClass");

    anotherCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "f11", YTType.STRING));
    anotherCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "f22", YTType.STRING));

    compositeIndex.addIndex(anotherCompositeIndex);

    final Object result = compositeIndex.createValue(db, "12", "test", "tset");
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test", "tset")));
  }

  @Test
  public void testDocumentToIndexSuccessful() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", "test");

    final Object result = compositeIndex.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testDocumentToIndexMapValueSuccessful() {
    final YTEntityImpl document = new YTEntityImpl();

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    document.field("fOne", 12);
    document.field("fTwo", stringMap);

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    final Collection<OCompositeKey> collectionResult = (Collection<OCompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key2")));
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulOne() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1));
    expectedResult.add(new OCompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyOne() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Collections.emptyList());

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertNull(result);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyTwo() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", Collections.emptyList());
    document.field("fTwo", 12);

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertNull(result);
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyOneNullValuesSupport() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Collections.emptyList());

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.setNullValuesIgnored(false);

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, List.of(new OCompositeKey(12, null)));
  }

  @Test
  public void testDocumentToIndexCollectionValueEmptyTwoNullValuesSupport() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", Collections.emptyList());
    document.field("fTwo", 12);

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.setNullValuesIgnored(false);

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);
    Assert.assertEquals(result, List.of(new OCompositeKey(null, 12)));
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulOne() {
    final YTEntityImpl document = new YTEntityImpl();

    final RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    document.field("fOne", 12);
    document.field("fTwo", ridBag);

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:10")));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11")));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11")));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulTwo() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(1, 12));
    expectedResult.add(new OCompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulTwo() {
    final RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", ridBag);

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(new YTRecordId("#1:10"), 12));
    expectedResult.add(new OCompositeKey(new YTRecordId("#1:11"), 12));
    expectedResult.add(new OCompositeKey(new YTRecordId("#1:11"), 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulThree() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));
    document.field("fThree", "test");

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.STRING));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1, "test"));
    expectedResult.add(new OCompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexRidBagValueSuccessfulThree() {
    final YTEntityImpl document = new YTEntityImpl();

    final RidBag ridBag = new RidBag(db);
    ridBag.add(new YTRecordId("#1:10"));
    ridBag.add(new YTRecordId("#1:11"));
    ridBag.add(new YTRecordId("#1:11"));

    document.field("fOne", 12);
    document.field("fTwo", ridBag);
    document.field("fThree", "test");

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.STRING));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(db, document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:10"), "test"));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11"), "test"));
    expectedResult.add(new OCompositeKey(12, new YTRecordId("#1:11"), "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expected = YTException.class)
  public void testDocumentToIndexCollectionValueTwoCollections() {
    final YTEntityImpl document = new YTEntityImpl();

    document.field("fOne", List.of(12));
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition =
        new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.INTEGER));
    compositeIndexDefinition.getDocumentValueToIndex(db, document);
  }

  @Test(expected = YTDatabaseException.class)
  public void testDocumentToIndexWrongField() {
    final YTEntityImpl document = new YTEntityImpl();

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
    final YTType[] result = compositeIndex.getTypes();

    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], YTType.INTEGER);
    Assert.assertEquals(result[1], YTType.STRING);
  }

  @Test
  public void testEmptyIndexReload() {
    final YTDatabaseDocumentTx database = new YTDatabaseDocumentTx("memory:compositetestone");
    database.create();

    final OCompositeIndexDefinition emptyCompositeIndex =
        new OCompositeIndexDefinition("testClass");

    emptyCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fOne", YTType.INTEGER));
    emptyCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fTwo", YTType.STRING));

    database.begin();
    final YTEntityImpl docToStore = emptyCompositeIndex.toStream(new YTEntityImpl());
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTEntityImpl docToLoad = database.load(docToStore.getIdentity());

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndex);
  }

  @Test
  public void testIndexReload() {
    final YTEntityImpl docToStore = compositeIndex.toStream(new YTEntityImpl());

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToStore);

    Assert.assertEquals(result, compositeIndex);
  }

  @Test
  public void testClassOnlyConstructor() {
    final YTDatabaseDocumentTx database = new YTDatabaseDocumentTx("memory:compositetesttwo");
    database.create();

    final OCompositeIndexDefinition emptyCompositeIndex =
        new OCompositeIndexDefinition(
            "testClass",
            Arrays.asList(
                new OPropertyIndexDefinition("testClass", "fOne", YTType.INTEGER),
                new OPropertyIndexDefinition("testClass", "fTwo", YTType.STRING)));

    final OCompositeIndexDefinition emptyCompositeIndexTwo =
        new OCompositeIndexDefinition("testClass");

    emptyCompositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("testClass", "fOne", YTType.INTEGER));
    emptyCompositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("testClass", "fTwo", YTType.STRING));

    Assert.assertEquals(emptyCompositeIndex, emptyCompositeIndexTwo);

    database.begin();
    final YTEntityImpl docToStore = emptyCompositeIndex.toStream(new YTEntityImpl());
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTEntityImpl docToLoad = database.load(docToStore.getIdentity());

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndexTwo);
  }

  @Test
  public void testProcessChangeListEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.STRING));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedList<String> trackedList = new TrackedList<String>(doc);
    trackedList.enableTracking(doc);
    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedList.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l3", 3)));
  }

  @Test
  public void testProcessChangeRidBagEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final RidBag ridBag = new RidBag(db);
    ridBag.enableTracking(null);
    ridBag.add(new YTRecordId("#10:0"));
    ridBag.add(new YTRecordId("#10:1"));
    ridBag.add(new YTRecordId("#10:0"));
    ridBag.add(new YTRecordId("#10:2"));
    ridBag.remove(new YTRecordId("#10:0"));
    ridBag.remove(new YTRecordId("#10:1"));

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        ridBag.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, new YTRecordId("#10:0"), 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, new YTRecordId("#10:2"), 3)));
  }

  @Test
  public void testProcessChangeListEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.STRING));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedList<String> trackedList = new TrackedList<String>(doc);

    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    trackedList.enableTracking(doc);
    trackedList.add("l4");
    trackedList.remove("l1");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedList.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "l1", 3)));
  }

  @Test
  public void testProcessChangeRidBagEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyRidBagIndexDefinition("testCollectionClass", "fTwo"));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final RidBag ridBag = new RidBag(db);

    ridBag.add(new YTRecordId("#10:1"));
    ridBag.add(new YTRecordId("#10:2"));
    ridBag.add(new YTRecordId("#10:3"));
    ridBag.remove(new YTRecordId("#10:2"));
    ridBag.disableTracking(null);
    ridBag.enableTracking(null);

    ridBag.add(new YTRecordId("#10:4"));
    ridBag.remove(new YTRecordId("#10:1"));

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        ridBag.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, new YTRecordId("#10:4"), 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, new YTRecordId("#10:1"), 3)));
  }

  @Test
  public void testProcessChangeSetEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.STRING));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedSet = new TrackedSet<String>(doc);

    trackedSet.enableTracking(doc);
    trackedSet.add("l1");
    trackedSet.add("l2");
    trackedSet.add("l3");
    trackedSet.remove("l2");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedSet.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l3", 3)));
  }

  @Test
  public void testProcessChangeSetEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyListIndexDefinition("testCollectionClass", "fTwo", YTType.STRING));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedSet<String> trackedMap = new TrackedSet<String>(doc);

    trackedMap.add("l1");
    trackedMap.add("l2");
    trackedMap.add("l3");
    trackedMap.remove("l2");

    trackedMap.enableTracking(doc);
    trackedMap.add("l4");
    trackedMap.remove("l1");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "l1", 3)));
  }

  @Test
  public void testProcessChangeKeyMapEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);
    trackedMap.enableTracking(doc);
    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k3", 3)));
  }

  @Test
  public void testProcessChangeKeyMapEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fOne", YTType.INTEGER));
    compositeIndexDefinition.addIndex(
        new OPropertyMapIndexDefinition(
            "testCollectionClass", "fTwo", YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(
        new OPropertyIndexDefinition("testCollectionClass", "fThree", YTType.INTEGER));

    final YTEntityImpl doc = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final TrackedMap<String> trackedMap = new TrackedMap<String>(doc);

    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");
    trackedMap.enableTracking(doc);

    trackedMap.put("k4", "v4");
    trackedMap.remove("k1");

    Object2IntOpenHashMap<OCompositeKey> keysToAdd = new Object2IntOpenHashMap<>();
    keysToAdd.defaultReturnValue(-1);

    Object2IntOpenHashMap<OCompositeKey> keysToRemove = new Object2IntOpenHashMap<>();
    keysToRemove.defaultReturnValue(-1);

    for (OMultiValueChangeEvent<Object, Object> multiValueChangeEvent :
        trackedMap.getTimeLine().getMultiValueChangeEvents()) {
      compositeIndexDefinition.processChangeEvent(
          db, multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);
    }

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "k1", 3)));
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", compositeIndex.getClassName());
  }
}
