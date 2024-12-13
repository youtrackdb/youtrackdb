package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ClassIndexManagerTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public ClassIndexManagerTest(@Optional Boolean remote) {
    super(remote != null ? remote : false);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();

    if (schema.existsClass("classIndexManagerTestClass")) {
      schema.dropClass("classIndexManagerTestClass");
    }

    if (schema.existsClass("classIndexManagerTestClassTwo")) {
      schema.dropClass("classIndexManagerTestClassTwo");
    }

    if (schema.existsClass("classIndexManagerTestSuperClass")) {
      schema.dropClass("classIndexManagerTestSuperClass");
    }

    if (schema.existsClass("classIndexManagerTestCompositeCollectionClass")) {
      schema.dropClass("classIndexManagerTestCompositeCollectionClass");
    }

    final SchemaClass superClass = schema.createClass("classIndexManagerTestSuperClass");
    superClass.createProperty(database, "prop0", PropertyType.STRING);
    superClass.createIndex(database,
        "classIndexManagerTestSuperClass.prop0",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"prop0"});

    final SchemaClass oClass = schema.createClass("classIndexManagerTestClass", superClass);
    oClass.createProperty(database, "prop1", PropertyType.STRING);
    oClass.createIndex(database,
        "classIndexManagerTestClass.prop1",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"prop1"});

    final Property propTwo = oClass.createProperty(database, "prop2", PropertyType.INTEGER);
    propTwo.createIndex(database, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createProperty(database, "prop3", PropertyType.BOOLEAN);

    final Property propFour = oClass.createProperty(database, "prop4", PropertyType.EMBEDDEDLIST,
        PropertyType.STRING);
    propFour.createIndex(database, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createProperty(database, "prop5", PropertyType.EMBEDDEDMAP, PropertyType.STRING);
    oClass.createIndex(database, "classIndexManagerTestIndexByKey",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "prop5");
    oClass.createIndex(database,
        "classIndexManagerTestIndexByValue", SchemaClass.INDEX_TYPE.NOTUNIQUE, "prop5 by value");

    final Property propSix = oClass.createProperty(database, "prop6", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    propSix.createIndex(database, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createIndex(database,
        "classIndexManagerComposite",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop1", "prop2"});

    final SchemaClass oClassTwo = schema.createClass("classIndexManagerTestClassTwo");
    oClassTwo.createProperty(database, "prop1", PropertyType.STRING);
    oClassTwo.createProperty(database, "prop2", PropertyType.INTEGER);

    final SchemaClass compositeCollectionClass =
        schema.createClass("classIndexManagerTestCompositeCollectionClass");
    compositeCollectionClass.createProperty(database, "prop1", PropertyType.STRING);
    compositeCollectionClass.createProperty(database, "prop2", PropertyType.EMBEDDEDLIST,
        PropertyType.INTEGER);

    compositeCollectionClass.createIndex(database,
        "classIndexManagerTestIndexValueAndCollection",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"prop1", "prop2"});

    oClass.createIndex(database,
        "classIndexManagerTestIndexOnPropertiesFromClassAndSuperclass",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"prop0", "prop1"});

    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.begin();
    database.command("delete from classIndexManagerTestClass").close();
    database.commit();

    database.begin();
    database.command("delete from classIndexManagerTestClassTwo").close();
    database.commit();

    database.begin();
    database.command("delete from classIndexManagerTestSuperClass").close();
    database.commit();

    if (!database.getStorage().isRemote()) {
      Assert.assertEquals(
          database
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(database, "classIndexManagerTestClass.prop1")
              .getInternal()
              .size(database),
          0);
      Assert.assertEquals(
          database
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(database, "classIndexManagerTestClass.prop2")
              .getInternal()
              .size(database),
          0);
    }

    super.afterMethod();
  }

  public void testPropertiesCheckUniqueIndexDubKeysCreate() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    docOne.field("prop1", "a");
    database.begin();
    docOne.save();
    database.commit();

    boolean exceptionThrown = false;
    try {
      docTwo.field("prop1", "a");
      database.begin();
      docTwo.save();
      database.commit();

    } catch (RecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullCreate() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    docOne.field("prop1", "a");
    database.begin();
    docOne.save();
    database.commit();

    docTwo.field("prop1", (String) null);
    database.begin();
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullCreateInTx() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    database.begin();
    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckUniqueIndexInParentDubKeysCreate() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    docOne.field("prop0", "a");
    database.begin();
    docOne.save();
    database.commit();

    boolean exceptionThrown = false;
    try {
      docTwo.field("prop0", "a");
      database.begin();
      docTwo.save();
      database.commit();
    } catch (RecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeysUpdate() {
    database.begin();
    EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    boolean exceptionThrown = false;
    docOne.field("prop1", "a");

    docOne.save();
    database.commit();

    database.begin();
    docTwo.field("prop1", "b");

    docTwo.save();
    database.commit();

    try {
      database.begin();
      docTwo = database.bindToSession(docTwo);
      docTwo.field("prop1", "a");

      docTwo.save();
      database.commit();
    } catch (RecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullUpdate() {
    database.begin();
    EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    docOne.field("prop1", "a");

    docOne.save();
    database.commit();

    database.begin();
    docTwo.field("prop1", "b");

    docTwo.save();
    database.commit();

    database.begin();
    docTwo = database.bindToSession(docTwo);
    docTwo.field("prop1", (String) null);

    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullUpdateInTX() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");

    database.begin();
    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", "b");
    docTwo.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckNonUniqueIndexDubKeys() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    docOne.field("prop2", 1);
    database.begin();
    docOne.save();
    database.commit();

    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");
    docTwo.field("prop2", 1);
    database.begin();
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckUniqueNullKeys() {
    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    database.begin();
    docOne.save();
    database.commit();

    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");
    database.begin();
    docTwo.save();
    database.commit();
  }

  public void testCreateDocumentWithoutClass() {
    checkEmbeddedDB();

    final Collection<? extends Index> beforeIndexes =
        database.getMetadata().getIndexManagerInternal().getIndexes(database);
    final Map<String, Long> indexSizeMap = new HashMap<>();

    for (final Index index : beforeIndexes) {
      indexSizeMap.put(index.getName(), index.getInternal().size(database));
    }

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.field("prop1", "a");
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.field("prop1", "a");
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final Collection<? extends Index> afterIndexes =
        database.getMetadata().getIndexManagerInternal().getIndexes(database);
    for (final Index index : afterIndexes) {
      Assert.assertEquals(
          index.getInternal().size(database), indexSizeMap.get(index.getName()).longValue());
    }
  }

  public void testUpdateDocumentWithoutClass() {
    checkEmbeddedDB();

    final Collection<? extends Index> beforeIndexes =
        database.getMetadata().getIndexManagerInternal().getIndexes(database);
    final Map<String, Long> indexSizeMap = new HashMap<>();

    for (final Index index : beforeIndexes) {
      indexSizeMap.put(index.getName(), index.getInternal().size(database));
    }

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.field("prop1", "a");
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.field("prop1", "b");
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    docOne.field("prop1", "a");
    docOne.save();
    database.commit();

    final Collection<? extends Index> afterIndexes =
        database.getMetadata().getIndexManagerInternal().getIndexes(database);
    for (final Index index : afterIndexes) {
      Assert.assertEquals(
          index.getInternal().size(database), indexSizeMap.get(index.getName()).longValue());
    }
  }

  public void testDeleteDocumentWithoutClass() {
    final EntityImpl docOne = new EntityImpl();
    docOne.field("prop1", "a");

    database.begin();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    database.bindToSession(docOne).delete();
    database.commit();
  }

  public void testDeleteModifiedDocumentWithoutClass() {
    EntityImpl docOne = new EntityImpl();
    docOne.field("prop1", "a");

    database.begin();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    docOne = database.bindToSession(docOne);
    docOne.field("prop1", "b");
    docOne.delete();
    database.commit();
  }

  public void testDocumentUpdateWithoutDirtyFields() {
    database.begin();
    EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    docOne.field("prop1", "a");

    docOne.save();
    database.commit();

    database.begin();
    docOne = database.bindToSession(docOne);
    docOne.setDirty();
    docOne.save();
    database.commit();
  }

  public void testCreateDocumentIndexRecordAdded() {
    checkEmbeddedDB();

    final EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    database.begin();
    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");
    final SchemaClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    try (Stream<RID> stream = propOneIndex.getInternal().getRids(database, "a")) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);

    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");

    final IndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();
    try (Stream<RID> rids =
        compositeIndex
            .getInternal()
            .getRids(database, compositeIndexDefinition.createValue(database, "a", 1))) {
      Assert.assertTrue(rids.findFirst().isPresent());
    }
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);

    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");
    try (Stream<RID> stream = propZeroIndex.getInternal().getRids(database, "x")) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);
  }

  public void testUpdateDocumentIndexRecordRemoved() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");
    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);

    database.begin();
    doc = database.bindToSession(doc);
    doc.removeField("prop2");
    doc.removeField("prop0");

    doc.save();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 0);
  }

  public void testUpdateDocumentNullKeyIndexRecordRemoved() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");
    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);

    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop2", (Object) null);
    doc.field("prop0", (Object) null);
    doc.save();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 0);
  }

  public void testUpdateDocumentIndexRecordUpdated() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");

    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");
    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");
    final IndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);

    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop2", 2);
    doc.field("prop0", "y");
    doc.save();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);

    try (Stream<RID> stream = propZeroIndex.getInternal().getRids(database, "y")) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    try (Stream<RID> stream = propOneIndex.getInternal().getRids(database, "a")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream =
        compositeIndex
            .getInternal()
            .getRids(database, compositeIndexDefinition.createValue(database, "a", 2))) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testUpdateDocumentIndexRecordUpdatedFromNullField() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");
    final IndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);

    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop2", 2);
    doc.save();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);

    try (Stream<RID> stream = propOneIndex.getInternal().getRids(database, "a")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream =
        compositeIndex
            .getInternal()
            .getRids(database, compositeIndexDefinition.createValue(database, "a", 2))) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
  }

  public void testListUpdate() {
    checkEmbeddedDB();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("classIndexManagerTestClass");

    final Index propFourIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop4");

    Assert.assertEquals(propFourIndex.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final List<String> listProperty = new ArrayList<>();
    listProperty.add("value1");
    listProperty.add("value2");

    doc.field("prop4", listProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propFourIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    List<String> trackedList = doc.field("prop4");
    trackedList.set(0, "value3");

    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.remove("value4");
    trackedList.remove("value2");
    trackedList.add("value5");

    doc.save();
    database.commit();

    Assert.assertEquals(propFourIndex.getInternal().size(database), 3);
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value4")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testMapUpdate() {
    checkEmbeddedDB();

    final Index propFiveIndexKey = database.getMetadata().getIndexManagerInternal()
        .getIndex(database,
            "classIndexManagerTestIndexByKey");
    final Index propFiveIndexValue = database.getMetadata().getIndexManagerInternal()
        .getIndex(database,
            "classIndexManagerTestIndexByValue");

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final Map<String, String> mapProperty = new HashMap<>();
    mapProperty.put("key1", "value1");
    mapProperty.put("key2", "value2");

    doc.field("prop5", mapProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 2);
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    Map<String, String> trackedMap = doc.field("prop5");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.remove("key1");
    trackedMap.put("key1", "value5");
    trackedMap.remove("key2");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value6");
    trackedMap.put("key8", "value6");
    trackedMap.put("key4", "value7");

    trackedMap.remove("key8");

    doc.save();
    database.commit();

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 5);
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key4")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key6")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key7")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    Assert.assertEquals(propFiveIndexValue.getInternal().size(database), 4);
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value7")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value6")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testSetUpdate() {
    checkEmbeddedDB();

    final Index propSixIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop6");

    Assert.assertEquals(propSixIndex.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final Set<String> setProperty = new HashSet<>();
    setProperty.add("value1");
    setProperty.add("value2");

    doc.field("prop6", setProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propSixIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    Set<String> trackedSet = doc.field("prop6");

    //noinspection OverwrittenKey
    trackedSet.add("value4");
    //noinspection OverwrittenKey
    trackedSet.add("value4");
    //noinspection OverwrittenKey
    trackedSet.add("value4");
    trackedSet.remove("value4");
    trackedSet.remove("value2");
    trackedSet.add("value5");

    doc.save();
    database.commit();

    Assert.assertEquals(propSixIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testListDelete() {
    checkEmbeddedDB();

    final Index propFourIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop4");

    Assert.assertEquals(propFourIndex.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final List<String> listProperty = new ArrayList<>();
    listProperty.add("value1");
    listProperty.add("value2");

    doc.field("prop4", listProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propFourIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    List<String> trackedList = doc.field("prop4");
    trackedList.set(0, "value3");

    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.remove("value4");
    trackedList.remove("value2");
    trackedList.add("value5");

    doc.save();
    database.commit();

    Assert.assertEquals(propFourIndex.getInternal().size(database), 3);
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value4")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFourIndex.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    trackedList = doc.field("prop4");
    trackedList.remove("value3");
    trackedList.remove("value4");
    trackedList.add("value8");

    doc.delete();
    database.commit();

    Assert.assertEquals(propFourIndex.getInternal().size(database), 0);
  }

  public void testMapDelete() {
    checkEmbeddedDB();

    final Index propFiveIndexKey = database.getMetadata().getIndexManagerInternal()
        .getIndex(database,
            "classIndexManagerTestIndexByKey");
    final Index propFiveIndexValue = database.getMetadata().getIndexManagerInternal()
        .getIndex(database,
            "classIndexManagerTestIndexByValue");

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final Map<String, String> mapProperty = new HashMap<>();
    mapProperty.put("key1", "value1");
    mapProperty.put("key2", "value2");

    doc.field("prop5", mapProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 2);
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    Map<String, String> trackedMap = doc.field("prop5");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.remove("key1");
    trackedMap.put("key1", "value5");
    trackedMap.remove("key2");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value6");
    trackedMap.put("key8", "value6");
    trackedMap.put("key4", "value7");

    trackedMap.remove("key8");

    doc.save();
    database.commit();

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 5);
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key4")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key6")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexKey.getInternal().getRids(database, "key7")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    Assert.assertEquals(propFiveIndexValue.getInternal().size(database), 4);
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value7")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propFiveIndexValue.getInternal().getRids(database, "value6")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    trackedMap = doc.field("prop5");

    trackedMap.remove("key1");
    trackedMap.remove("key3");
    trackedMap.remove("key4");
    trackedMap.put("key6", "value10");
    trackedMap.put("key11", "value11");

    doc.delete();
    database.commit();

    Assert.assertEquals(propFiveIndexKey.getInternal().size(database), 0);
    Assert.assertEquals(propFiveIndexValue.getInternal().size(database), 0);
  }

  public void testSetDelete() {
    checkEmbeddedDB();
    final Index propSixIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop6");

    Assert.assertEquals(propSixIndex.getInternal().size(database), 0);

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");

    final Set<String> setProperty = new HashSet<>();
    setProperty.add("value1");
    setProperty.add("value2");

    doc.field("prop6", setProperty);
    doc.save();
    database.commit();

    Assert.assertEquals(propSixIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value2")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    Set<String> trackedSet = doc.field("prop6");

    //noinspection OverwrittenKey
    trackedSet.add("value4");
    //noinspection OverwrittenKey
    trackedSet.add("value4");
    //noinspection OverwrittenKey
    trackedSet.add("value4");
    trackedSet.remove("value4");
    trackedSet.remove("value2");
    trackedSet.add("value5");

    doc.save();
    database.commit();

    Assert.assertEquals(propSixIndex.getInternal().size(database), 2);
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value1")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = propSixIndex.getInternal().getRids(database, "value5")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();
    doc = database.bindToSession(doc);
    trackedSet = doc.field("prop6");
    trackedSet.remove("value1");
    trackedSet.add("value6");

    doc.delete();
    database.commit();

    Assert.assertEquals(propSixIndex.getInternal().size(database), 0);
  }

  public void testDeleteDocumentIndexRecordDeleted() {
    checkEmbeddedDB();

    final EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    database.begin();
    doc.save();
    database.commit();

    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");
    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");

    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);
    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(propZeroIndex.getInternal().size(database), 0);
    Assert.assertEquals(propOneIndex.getInternal().size(database), 0);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
  }

  public void testDeleteUpdatedDocumentIndexRecordDeleted() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();
    database.commit();

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");

    final Index propZeroIndex = database.getMetadata().getIndexManagerInternal().getIndex(database,
        "classIndexManagerTestSuperClass.prop0");
    Assert.assertEquals(propZeroIndex.getInternal().size(database), 1);
    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 1);

    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop2", 2);
    doc.field("prop0", "y");

    doc.delete();
    database.commit();

    Assert.assertEquals(propZeroIndex.getInternal().size(database), 0);
    Assert.assertEquals(propOneIndex.getInternal().size(database), 0);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
  }

  public void testDeleteUpdatedDocumentNullFieldIndexRecordDeleted() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();
    database.commit();

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 0);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
  }

  public void testDeleteUpdatedDocumentOrigNullFieldIndexRecordDeleted() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();
    database.commit();

    final Index propOneIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerTestClass.prop1");
    final Index compositeIndex = database.getMetadata().getIndexManagerInternal()
        .getIndex(database, "classIndexManagerComposite");

    Assert.assertEquals(propOneIndex.getInternal().size(database), 1);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);

    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop2", 2);

    doc.delete();
    database.commit();

    Assert.assertEquals(propOneIndex.getInternal().size(database), 0);
    Assert.assertEquals(compositeIndex.getInternal().size(database), 0);
  }

  public void testNoClassIndexesUpdate() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClassTwo");
    doc.field("prop1", "a");

    doc.save();
    database.commit();
    database.begin();
    doc = database.bindToSession(doc);
    doc.field("prop1", "b");

    doc.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClassInternal oClass = (SchemaClassInternal) schema.getClass(
        "classIndexManagerTestClass");

    final Collection<Index> indexes = oClass.getIndexesInternal(database);
    for (final Index index : indexes) {
      Assert.assertEquals(index.getInternal().size(database), 0);
    }
  }

  public void testNoClassIndexesDelete() {
    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestClassTwo");
    doc.field("prop1", "a");

    doc.save();
    database.commit();

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  public void testCollectionCompositeCreation() {
    checkEmbeddedDB();

    final EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    database.begin();
    doc.save();
    database.commit();

    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 1))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 2))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeNullSimpleFieldCreation() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", (Object) null);
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  public void testCollectionCompositeNullCollectionFieldCreation() {
    checkEmbeddedDB();

    final EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", (Object) null);

    database.begin();
    doc.save();
    database.commit();

    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  public void testCollectionCompositeUpdateSimpleField() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop1", "test2");

    doc.save();
    database.commit();

    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 1))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 2))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }

    Assert.assertEquals(index.getInternal().size(database), 2);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasAssigned() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop2", Arrays.asList(1, 3));

    doc.save();
    database.commit();

    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 1))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 3))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }

    Assert.assertEquals(index.getInternal().size(database), 2);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChanged() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.save();
    database.commit();

    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 2))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 3))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 4))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test1", 5))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }

    Assert.assertEquals(index.getInternal().size(database), 4);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChangedSimpleFieldWasAssigned() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.field("prop1", "test2");

    doc.save();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 4);

    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 2))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 3))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 4))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }
    try (Stream<RID> stream = index.getInternal()
        .getRids(database, new CompositeKey("test2", 5))) {
      Assert.assertEquals(stream.findAny().orElse(null), doc.getIdentity());
    }

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateSimpleFieldNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop1", (Object) null);

    doc.save();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasAssignedNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop2", (Object) null);

    doc.save();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateBothAssignedNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc.field("prop2", (Object) null);
    doc.field("prop1", (Object) null);

    doc.save();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChangedSimpleFieldWasAssignedNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.field("prop1", (Object) null);

    doc.save();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteSimpleFieldAssigend() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop1", "test2");

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldAssigend() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop2", Arrays.asList(1, 3));

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldChanged() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteBothCollectionSimpleFieldChanged() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.field("prop1", "test2");

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteBothCollectionSimpleFieldAssigend() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop2", Arrays.asList(1, 3));
    doc.field("prop1", "test2");

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteSimpleFieldNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc.field("prop1", (Object) null);

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc = database.bindToSession(doc);
    doc.field("prop2", (Object) null);

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteBothSimpleCollectionFieldNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    doc.field("prop2", (Object) null);
    doc.field("prop1", (Object) null);

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldChangedSimpleFieldNull() {
    checkEmbeddedDB();

    database.begin();
    EntityImpl doc = new EntityImpl("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    final Index index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.field("prop1", (Object) null);

    doc.delete();
    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexOnPropertiesFromClassAndSuperclass() {
    checkEmbeddedDB();

    final EntityImpl docOne = new EntityImpl("classIndexManagerTestClass");
    docOne.field("prop0", "doc1-prop0");
    docOne.field("prop1", "doc1-prop1");

    database.begin();
    docOne.save();
    database.commit();

    final EntityImpl docTwo = new EntityImpl("classIndexManagerTestClass");
    docTwo.field("prop0", "doc2-prop0");
    docTwo.field("prop1", "doc2-prop1");

    database.begin();
    docTwo.save();
    database.commit();

    final Schema schema = database.getMetadata().getSchema();
    final Index index =
        database.getMetadata().getIndexManagerInternal()
            .getIndex(database, "classIndexManagerTestIndexOnPropertiesFromClassAndSuperclass");

    Assert.assertEquals(index.getInternal().size(database), 2);
  }
}
