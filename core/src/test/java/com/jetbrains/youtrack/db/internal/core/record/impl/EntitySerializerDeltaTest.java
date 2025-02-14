package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.EntitySerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class EntitySerializerDeltaTest extends DbTestBase {

  @Test
  public void testGetFromOriginalSimpleDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    var constantFieldName = "constantField";
    var originalValue = "orValue";
    var testValue = "testValue";
    var removeField = "removeField";

    doc.setProperty(fieldName, originalValue);
    doc.setProperty(constantFieldName, "someValue");
    doc.setProperty(removeField, "removeVal");

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    doc.setProperty(fieldName, testValue);
    doc.removeProperty(removeField);
    // test serialization/deserialization
    var delta = DocumentSerializerDelta.instance();
    var bytes = delta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    delta.deserializeDelta(session, bytes, doc);
    assertEquals(testValue, doc.field(fieldName));
    assertNull(doc.field(removeField));
    session.rollback();
  }

  @Test
  public void testGetFromNestedDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var nestedDoc = (EntityImpl) session.newEmbededEntity(claz.getName(session));
    var fieldName = "testField";
    var constantFieldName = "constantField";
    var originalValue = "orValue";
    var testValue = "testValue";
    var nestedDocField = "nestedField";

    nestedDoc.setProperty(fieldName, originalValue);
    nestedDoc.setProperty(constantFieldName, "someValue1");

    doc.setProperty(constantFieldName, "someValue2");
    doc.setProperty(nestedDocField, nestedDoc, PropertyType.EMBEDDED);

    var originalDoc = (EntityImpl) session.newEntity();
    originalDoc.setProperty(constantFieldName, "someValue2");
    originalDoc.setProperty(nestedDocField, nestedDoc, PropertyType.EMBEDDED);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    nestedDoc = doc.field(nestedDocField);
    nestedDoc.setProperty(fieldName, testValue);

    doc.setProperty(nestedDocField, nestedDoc, PropertyType.EMBEDDED);

    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    // test serialization/deserialization
    originalDoc = session.bindToSession(originalDoc);
    serializerDelta.deserializeDelta(session, bytes, originalDoc);
    nestedDoc = originalDoc.field(nestedDocField);
    assertEquals(testValue, nestedDoc.field(fieldName));
    session.rollback();
  }

  @Test
  public void testListDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var fieldName = "testField";
    List<String> originalValue = new ArrayList<>();
    originalValue.add("one");
    originalValue.add("two");
    originalValue.add("toRemove");

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    List<String> newArray = doc.field(fieldName);
    newArray.set(1, "three");
    newArray.remove("toRemove");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();

    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    List<?> checkList = doc.getProperty(fieldName);
    assertEquals("three", checkList.get(1));
    assertFalse(checkList.contains("toRemove"));
    session.rollback();
  }

  @Test
  public void testSetDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    var doc = (EntityImpl) session.newEntity(claz);

    var fieldName = "testField";
    Set<String> originalValue = new HashSet<>();
    originalValue.add("one");
    originalValue.add("toRemove");

    session.begin();
    doc.setProperty(fieldName, originalValue);
    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    Set<String> newArray = doc.field(fieldName);
    newArray.add("three");
    newArray.remove("toRemove");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    Set<String> checkSet = doc.field(fieldName);
    assertTrue(checkSet.contains("three"));
    assertFalse(checkSet.contains("toRemove"));
    session.rollback();
  }

  @Test
  public void testSetOfSetsDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    Set<Set<String>> originalValue = new HashSet<>();
    for (var i = 0; i < 2; i++) {
      Set<String> containedSet = new HashSet<>();
      containedSet.add("one");
      containedSet.add("two");
      originalValue.add(containedSet);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    @SuppressWarnings("unchecked")
    var newSet = ((Set<Set<String>>) doc.getProperty(fieldName)).iterator().next();
    newSet.add("three");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    Set<Set<String>> checkSet = doc.field(fieldName);
    assertTrue(checkSet.iterator().next().contains("three"));
    session.rollback();
  }

  @Test
  public void testListOfListsDelta() {

    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    List<List<String>> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      List<String> containedList = new ArrayList<>();
      containedList.add("one");
      containedList.add("two");

      originalValue.add(containedList);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    var newList = doc.<List<List<String>>>field(fieldName).getFirst();
    newList.set(1, "three");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    List<List<String>> checkList = doc.field(fieldName);
    assertEquals("three", checkList.getFirst().get(1));
    session.rollback();
  }

  @Test
  public void testListOfDocsDelta() {
    var fieldName = "testField";

    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var constantField = "constField";
    var constValue = "ConstValue";
    var variableField = "varField";
    List<EntityImpl> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      var containedDoc = (EntityImpl) session.newEmbededEntity();
      containedDoc.setProperty(constantField, constValue);
      containedDoc.setProperty(variableField, "one" + i);
      originalValue.add(containedDoc);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    @SuppressWarnings("unchecked")
    var testDoc = ((List<EntityImpl>) doc.getProperty(fieldName)).get(1);
    testDoc.setProperty(variableField, "two");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    List<EntityImpl> checkList = doc.field(fieldName);
    var checkDoc = checkList.get(1);
    assertEquals(constValue, checkDoc.field(constantField));
    assertEquals("two", checkDoc.field(variableField));
    session.rollback();
  }

  @Test
  public void testListOfListsOfDocumentDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    var constantField = "constField";
    var constValue = "ConstValue";
    var variableField = "varField";

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    List<List<EntityImpl>> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      List<EntityImpl> containedList = new ArrayList<>();
      var d1 = (EntityImpl) session.newEntity();
      d1.setProperty(constantField, constValue);
      d1.setProperty(variableField, "one");
      var d2 = (EntityImpl) session.newEntity();
      d2.setProperty(constantField, constValue);
      containedList.add(d1);
      containedList.add(d2);
      originalValue.add(containedList);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    originalValue = doc.getProperty(fieldName);
    var d1 = originalValue.getFirst().getFirst();
    d1.setProperty(variableField, "two");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    List<List<EntityImpl>> checkList = doc.field(fieldName);
    assertEquals("two", checkList.getFirst().getFirst().field(variableField));
    session.rollback();
  }

  @Test
  public void testListOfListsOfListDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    List<List<List<String>>> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      List<List<String>> containedList = new ArrayList<>();
      for (var j = 0; j < 2; j++) {
        List<String> innerList = new ArrayList<>();
        innerList.add("el1" + j + i);
        innerList.add("el2" + j + i);
        containedList.add(innerList);
      }
      originalValue.add(containedList);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    @SuppressWarnings("unchecked")
    var innerList = ((List<List<List<String>>>) doc.field(fieldName)).getFirst()
        .getFirst();
    innerList.set(0, "changed");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);
    List<List<List<String>>> checkList = doc.field(fieldName);
    assertEquals("changed", checkList.getFirst().getFirst().getFirst());
    session.rollback();
  }

  @Test
  public void testListOfDocsWithList() {
    var fieldName = "testField";

    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var constantField = "constField";
    var constValue = "ConstValue";
    var variableField = "varField";

    List<EntityImpl> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      var containedDoc = (EntityImpl) session.newEmbededEntity();
      containedDoc.setProperty(constantField, constValue);
      List<String> listField = new ArrayList<>();
      for (var j = 0; j < 2; j++) {
        listField.add("Some" + j);
      }
      containedDoc.setProperty(variableField, listField);
      originalValue.add(containedDoc);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    @SuppressWarnings("unchecked")
    var testDoc = ((List<EntityImpl>) doc.field(fieldName)).get(1);
    List<String> currentList = testDoc.field(variableField);
    currentList.set(0, "changed");
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);

    List<EntityImpl> checkList = doc.field(fieldName);
    var checkDoc = checkList.get(1);
    List<String> checkInnerList = checkDoc.field(variableField);
    assertEquals("changed", checkInnerList.getFirst());
    session.rollback();
  }

  @Test
  public void testListAddDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var fieldName = "testField";
    List<String> originalValue = new ArrayList<>();
    originalValue.add("one");
    originalValue.add("two");
    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    List<String> newArray = doc.field(fieldName);
    newArray.add("three");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    List<String> checkList = doc.field(fieldName);
    assertEquals(3, checkList.size());
    session.rollback();
  }

  @Test
  public void testListOfListAddDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var fieldName = "testField";
    List<List<String>> originalList = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      List<String> nestedList = new ArrayList<>();
      nestedList.add("one");
      nestedList.add("two");
      originalList.add(nestedList);
    }

    doc.setProperty(fieldName, originalList);

    doc = session.save(doc);
    session.commit();

    // Deep Copy is not working in this case, use toStream/fromStream as workaround.
    // EntityImpl originalDoc = doc.copy();
    var originalDoc = (EntityImpl) session.newEntity();
    RecordInternal.unsetDirty(originalDoc);

    session.begin();
    doc = session.bindToSession(doc);
    originalDoc.fromStream(doc.toStream());

    @SuppressWarnings("unchecked")
    var newArray = ((List<List<String>>) doc.field(fieldName)).getFirst();
    newArray.add("three");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    serializerDelta.deserializeDelta(session, bytes, originalDoc);

    List<List<String>> rootList = originalDoc.field(fieldName);
    var checkList = rootList.getFirst();
    assertEquals(3, checkList.size());
    session.rollback();
  }

  @Test
  public void testListRemoveDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var fieldName = "testField";
    List<String> originalValue = new ArrayList<>();
    originalValue.add("one");
    originalValue.add("two");
    originalValue.add("three");

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    List<String> newArray = doc.field(fieldName);
    newArray.remove(0);
    newArray.remove(0);

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);
    List<String> checkList = doc.field(fieldName);
    assertEquals("three", checkList.getFirst());
    session.rollback();
  }

  @Test
  public void testAddDocFieldDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    var constantFieldName = "constantField";
    var testValue = "testValue";

    doc.setProperty(constantFieldName + "1", "someValue1");
    doc.setProperty(constantFieldName, "someValue");

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    doc.setProperty(fieldName, testValue);

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    assertEquals(testValue, doc.field(fieldName));
    session.rollback();
  }

  @Test
  public void testRemoveCreateDocFieldDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    var constantFieldName = "constantField";
    var testValue = "testValue";

    session.begin();
    doc.setProperty(fieldName, testValue);
    doc.setProperty(constantFieldName, "someValue");

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    doc.removeProperty(fieldName);
    doc.setProperty("other", "new");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);

    assertFalse(doc.hasProperty(fieldName));
    assertEquals("new", doc.getProperty("other"));
    session.rollback();
  }

  @Test
  public void testRemoveNestedDocFieldDelta() {
    var nestedFieldName = "nested";

    var claz = session.createClassIfNotExist("TestClass");
    claz.createProperty(session, nestedFieldName, PropertyType.EMBEDDED);

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    var constantFieldName = "constantField";
    var testValue = "testValue";

    doc.setProperty(fieldName, testValue);
    doc.setProperty(constantFieldName, "someValue");

    var rootDoc = (EntityImpl) session.newEntity(claz);
    rootDoc.setProperty(nestedFieldName, doc);

    rootDoc = session.save(rootDoc);
    session.commit();

    session.begin();
    rootDoc = session.bindToSession(rootDoc);

    doc = rootDoc.field(nestedFieldName);
    doc.removeProperty(fieldName);

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, rootDoc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    EntityImpl nested = doc.field(nestedFieldName);
    assertFalse(nested.hasProperty(fieldName));
    session.rollback();
  }

  @Test
  public void testRemoveFieldListOfDocsDelta() {
    var fieldName = "testField";

    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);

    var constantField = "constField";
    var constValue = "ConstValue";
    var variableField = "varField";
    List<EntityImpl> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      var containedDoc = (EntityImpl) session.newEntity();
      containedDoc.setProperty(constantField, constValue);
      containedDoc.setProperty(variableField, "one" + i);
      originalValue.add(containedDoc);
    }

    doc.setProperty(fieldName, originalValue);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    @SuppressWarnings("unchecked")
    EntityImpl testDoc = ((List<Identifiable>) doc.field(fieldName)).get(1).getRecord(session);
    testDoc.removeProperty(variableField);
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    List<Identifiable> checkList = doc.field(fieldName);
    EntityImpl checkDoc = checkList.get(1).getRecord(session);
    assertEquals(constValue, checkDoc.field(constantField));
    assertFalse(checkDoc.hasProperty(variableField));
    session.rollback();
  }

  @Test
  public void testUpdateEmbeddedMapDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put("first", "one");
    mapValue.put("second", "two");

    doc.setProperty(fieldName, mapValue, PropertyType.EMBEDDEDMAP);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    Map<String, String> containedMap = doc.field(fieldName);
    containedMap.put("first", "changed");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    containedMap = doc.field(fieldName);
    assertEquals("changed", containedMap.get("first"));
    session.rollback();
  }

  @Test
  public void testUpdateListOfEmbeddedMapDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    List<Map<String, String>> originalValue = new ArrayList<>();
    for (var i = 0; i < 2; i++) {
      Map<String, String> mapValue = new HashMap<>();
      mapValue.put("first", "one");
      mapValue.put("second", "two");
      originalValue.add(mapValue);
    }

    doc.setProperty(fieldName, originalValue, PropertyType.EMBEDDEDLIST);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    @SuppressWarnings("unchecked")
    var containedMap = ((List<Map<String, String>>) doc.field(
        fieldName)).getFirst();
    containedMap.put("first", "changed");
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    //noinspection unchecked
    containedMap = ((List<Map<String, String>>) doc.field(fieldName)).get(0);
    assertEquals("changed", containedMap.get("first"));
    //noinspection unchecked
    containedMap = ((List<Map<String, String>>) doc.field(fieldName)).get(1);
    assertEquals("one", containedMap.get("first"));
    session.rollback();
  }

  @Test
  public void testUpdateDocInMapDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    Map<String, EntityImpl> mapValue = new HashMap<>();
    var d1 = (EntityImpl) session.newEntity();
    d1.setProperty("f1", "v1");
    mapValue.put("first", d1);
    var d2 = (EntityImpl) session.newEntity();
    d2.setProperty("f2", "v2");
    mapValue.put("second", d2);
    doc.setProperty(fieldName, mapValue, PropertyType.EMBEDDEDMAP);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    Map<String, EntityImpl> containedMap = doc.field(fieldName);
    var changeDoc = containedMap.get("first");
    changeDoc.setProperty("f1", "changed");

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    containedMap = doc.field(fieldName);
    var containedDoc = containedMap.get("first");
    assertEquals("changed", containedDoc.field("f1"));
    session.rollback();
  }

  @Test
  public void testListOfMapsUpdateDelta() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";
    List<Map> originalList = new ArrayList<>();
    List<Map> copyList = new ArrayList<>();

    Map<String, String> mapValue1 = new HashMap<>();
    mapValue1.put("first", "one");
    mapValue1.put("second", "two");
    originalList.add(mapValue1);
    Map<String, String> mapValue1Copy = new HashMap<>(mapValue1);
    copyList.add(mapValue1Copy);

    Map<String, String> mapValue2 = new HashMap<>();
    mapValue2.put("third", "three");
    mapValue2.put("forth", "four");
    originalList.add(mapValue2);
    Map<String, String> mapValue2Copy = new HashMap<>(mapValue2);
    copyList.add(mapValue2Copy);

    doc.field(fieldName, originalList);

    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    var containedMap = (Map<String, String>) ((List) doc.field(
        fieldName)).getFirst();
    containedMap.put("first", "changed");
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);

    containedMap = (Map<String, String>) ((List) doc.field(fieldName)).getFirst();
    assertEquals("changed", containedMap.get("first"));
    session.rollback();
  }

  @Test
  public void testRidbagsUpdateDeltaAddWithCopy() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";

    var first = (EntityImpl) session.newEntity(claz);
    first = session.save(first);
    var second = (EntityImpl) session.newEntity(claz);
    second = session.save(second);

    var ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    doc = session.save(doc);

    var originalDoc = doc;
    doc.save();

    var third = (EntityImpl) session.newEntity(claz);
    third = session.save(third);
    session.commit();

    session.begin();
    first = session.bindToSession(first);
    second = session.bindToSession(second);
    third = session.bindToSession(third);

    ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    ridBag.add(third.getIdentity());

    doc = session.bindToSession(doc);
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    originalDoc = session.bindToSession(originalDoc);
    serializerDelta.deserializeDelta(session, bytes, originalDoc);

    RidBag mergedRidbag = originalDoc.field(fieldName);
    assertEquals(ridBag, mergedRidbag);
    session.rollback();
  }

  @Test
  public void testRidbagsUpdateDeltaRemoveWithCopy() {
    var claz = session.createClassIfNotExist("TestClass");

    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";

    session.begin();
    var first = (EntityImpl) session.newEntity(claz);
    first = session.save(first);
    var second = (EntityImpl) session.newEntity(claz);
    second = session.save(second);
    var third = (EntityImpl) session.newEntity(claz);
    third = session.save(third);
    session.commit();

    session.begin();
    first = session.bindToSession(first);
    second = session.bindToSession(second);
    third = session.bindToSession(third);

    var ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    ridBag.add(third.getIdentity());

    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    doc = session.save(doc);
    session.commit();

    session.begin();
    first = session.bindToSession(first);
    second = session.bindToSession(second);
    doc = session.bindToSession(doc);

    ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    RidBag mergedRidbag = doc.field(fieldName);
    assertEquals(ridBag, mergedRidbag);
    session.rollback();
  }

  @Test
  public void testRidbagsUpdateDeltaAdd() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";

    var first = (EntityImpl) session.newEntity(claz);
    first = session.save(first);
    var second = (EntityImpl) session.newEntity(claz);
    second = session.save(second);

    var ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    doc = session.save(doc);
    session.commit();

    doc = session.bindToSession(doc);

    session.begin();
    var third = (EntityImpl) session.newEntity(claz);
    third = session.save(third);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    third = session.bindToSession(third);

    ridBag = doc.getProperty(fieldName);
    ridBag.add(third.getIdentity());

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    RidBag mergedRidbag = doc.field(fieldName);
    assertEquals(ridBag, mergedRidbag);
    session.rollback();
  }

  @Test
  public void testRidbagsUpdateDeltaRemove() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";

    var first = (EntityImpl) session.newEntity(claz);
    first = session.save(first);
    var second = (EntityImpl) session.newEntity(claz);
    second = session.save(second);
    var third = (EntityImpl) session.newEntity(claz);
    third = session.save(third);

    var ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    ridBag.add(third.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    ridBag = doc.getProperty(fieldName);
    ridBag.remove(third.getIdentity());

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    RidBag mergedRidbag = doc.field(fieldName);
    assertEquals(ridBag, mergedRidbag);
    session.rollback();
  }

  @Test
  public void testRidbagsUpdateDeltaChangeWithCopy() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    var fieldName = "testField";

    var first = (EntityImpl) session.newEntity(claz);
    first = session.save(first);
    var second = (EntityImpl) session.newEntity(claz);
    second = session.save(second);
    var third = (EntityImpl) session.newEntity(claz);
    third = session.save(third);

    var ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(second.getIdentity());
    ridBag.add(third.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);
    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);

    ridBag = new RidBag(session);
    ridBag.add(first.getIdentity());
    ridBag.add(third.getIdentity());
    doc.field(fieldName, ridBag, PropertyType.LINKBAG);

    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();

    session.begin();
    serializerDelta.deserializeDelta(session, bytes, doc);
    RidBag mergedRidbag = doc.field(fieldName);
    assertEquals(ridBag, mergedRidbag);
    session.rollback();
  }

  @Test
  public void testDeltaNullValues() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    doc.setProperty("one", "value");
    doc.setProperty("list", List.of("test"));
    doc.setProperty("set", new HashSet<>(List.of("test")));
    Map<String, String> map = new HashMap<>();
    map.put("two", "value");
    doc.setProperty("map", map);
    Identifiable link = session.save(session.newEntity("testClass"));
    doc.setProperty("linkList", Collections.singletonList(link));
    doc.setProperty("linkSet", new HashSet<>(Collections.singletonList(link)));
    Map<String, Identifiable> linkMap = new HashMap<>();
    linkMap.put("two", link);
    doc.setProperty("linkMap", linkMap);
    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    doc.setProperty("one", null);
    doc.<List<String>>getProperty("list").add(null);
    doc.<Set<String>>getProperty("set").add(null);
    doc.<Map<String, String>>getProperty("map").put("nullValue", null);
    doc.<List<Identifiable>>getProperty("linkList").add(null);
    doc.<Set<Identifiable>>getProperty("linkSet").add(null);
    doc.<Map<String, Identifiable>>getProperty("linkMap").put("nullValue", null);
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();

    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);
    assertTrue(((List) doc.getProperty("list")).contains(null));
    assertTrue(((Set) doc.getProperty("set")).contains(null));
    assertTrue(((Map) doc.getProperty("map")).containsKey("nullValue"));
    assertTrue(((List) doc.getProperty("linkList")).contains(null));
    assertTrue(((Set) doc.getProperty("linkSet")).contains(null));
    assertTrue(((Map) doc.getProperty("linkMap")).containsKey("nullValue"));
    session.rollback();
  }

  @Test
  public void testDeltaLinkAllCases() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    Identifiable link = session.save(session.newEntity("testClass"));
    var link1 = session.save(session.newEntity("testClass"));
    doc.setProperty("linkList", Arrays.asList(link, link1, link1));
    doc.setProperty("linkSet", new HashSet<>(Arrays.asList(link, link1)));
    Map<String, Identifiable> linkMap = new HashMap<>();
    linkMap.put("one", link);
    linkMap.put("two", link1);
    linkMap.put("three", link1);
    doc.setProperty("linkMap", linkMap);
    doc = session.save(doc);

    EntityImpl link2 = session.save(session.newEntity("testClass"));
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    link2 = session.bindToSession(link2);
    link1 = session.bindToSession(link1);

    doc.<List<Identifiable>>getProperty("linkList").set(1, link2);
    doc.<List<Identifiable>>getProperty("linkList").remove(link1);
    doc.<List<Identifiable>>getProperty("linkList").add(link2);

    doc.<Set<Identifiable>>getProperty("linkSet").add(link2);
    doc.<Set<Identifiable>>getProperty("linkSet").remove(link1);
    doc.<Map<String, Identifiable>>getProperty("linkMap").put("new", link2);
    doc.<Map<String, Identifiable>>getProperty("linkMap").put("three", link2);
    doc.<Map<String, Identifiable>>getProperty("linkMap").remove("two");
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);
    assertFalse(doc.<List<Identifiable>>getProperty("linkList").contains(link1));
    assertTrue(doc.<List<Identifiable>>getProperty("linkList").contains(link2));
    assertEquals(doc.<List<Identifiable>>getProperty("linkList").get(1), link2);
    assertTrue(doc.<Set<Identifiable>>getProperty("linkSet").contains(link2));
    assertFalse(doc.<Set<Identifiable>>getProperty("linkSet").contains(link1));
    assertEquals(doc.<Map<String, Identifiable>>getProperty("linkMap").get("new"), link2);
    assertEquals(doc.<Map<String, Identifiable>>getProperty("linkMap").get("three"), link2);
    assertTrue(doc.<Map<String, Identifiable>>getProperty("linkMap").containsKey("one"));
    assertFalse(doc.<Map<String, Identifiable>>getProperty("linkMap").containsKey("two"));
    session.rollback();
  }

  @Test
  public void testDeltaAllCasesMap() {
    var claz = session.createClassIfNotExist("TestClass");

    session.begin();
    var doc = (EntityImpl) session.newEntity(claz);
    Map<String, String> map = new HashMap<>();
    map.put("two", "value");
    doc.setProperty("map", map);
    Map<String, String> map1 = new HashMap<>();
    map1.put("two", "value");
    map1.put("one", "other");
    Map<String, Map<String, String>> mapNested = new HashMap<>();
    Map<String, String> nested = new HashMap<>();
    nested.put("one", "value");
    mapNested.put("nest", nested);
    doc.setProperty("mapNested", mapNested);
    doc.setProperty("map1", map1);
    Map<String, Entity> mapEmbedded = new HashMap<>();
    var embedded = session.newEntity();
    embedded.setProperty("other", 1);
    mapEmbedded.put("first", embedded);
    doc.setProperty("mapEmbedded", mapEmbedded, PropertyType.EMBEDDEDMAP);
    doc = session.save(doc);
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    var embedded1 = session.newEntity();
    embedded1.setProperty("other", 1);
    doc.<Map<String, Entity>>getProperty("mapEmbedded").put("newDoc", embedded1);
    doc.<Map<String, String>>getProperty("map").put("value", "other");
    doc.<Map<String, String>>getProperty("map").put("two", "something");
    doc.<Map<String, String>>getProperty("map1").remove("one");
    doc.<Map<String, String>>getProperty("map1").put("two", "something");
    doc.<Map<String, Map<String, String>>>getProperty("mapNested")
        .get("nest")
        .put("other", "value");
    // test serialization/deserialization
    var serializerDelta = DocumentSerializerDelta.instance();
    var bytes = serializerDelta.serializeDelta(session, doc);
    session.rollback();
    session.begin();
    doc = session.bindToSession(doc);
    serializerDelta.deserializeDelta(session, bytes, doc);
    assertNotNull((doc.<Map<String, String>>getProperty("mapEmbedded")).get("newDoc"));
    assertEquals(
        doc.<Map<String, Entity>>getProperty("mapEmbedded")
            .get("newDoc")
            .getProperty("other"),
        Integer.valueOf(1));
    assertEquals("other", doc.<Map<String, String>>getProperty("map").get("value"));
    assertEquals("something", doc.<Map<String, String>>getProperty("map").get("two"));
    assertEquals("something", doc.<Map<String, String>>getProperty("map1").get("two"));
    assertNull(doc.<Map<String, String>>getProperty("map1").get("one"));
    assertEquals(
        "value",
        doc.<Map<String, Map<String, String>>>getProperty("mapNested")
            .get("nest")
            .get("other"));
    session.rollback();
  }

  @Test
  public void testSimpleSerialization() {
    var document = (EntityImpl) session.newEntity();

    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.field("heigth", 12.5f);
    document.field("bitHeigth", 12.5d);
    document.field("class", (byte) 'C');
    document.field("nullField", (Object) null);
    document.field("character", 'C');
    document.field("alive", true);
    document.field("dateTime", new Date());
    document.field(
        "bigNumber", new BigDecimal("43989872423376487952454365232141525434.32146432321442534"));
    var bag = new RidBag(session);
    bag.add(new RecordId(1, 1));
    bag.add(new RecordId(2, 2));
    // document.field("ridBag", bag);
    var c = Calendar.getInstance();
    document.field("date", c.getTime(), PropertyType.DATE);
    var c1 = Calendar.getInstance();
    c1.set(Calendar.MILLISECOND, 0);
    c1.set(Calendar.SECOND, 0);
    c1.set(Calendar.MINUTE, 0);
    c1.set(Calendar.HOUR_OF_DAY, 0);
    document.field("date1", c1.getTime(), PropertyType.DATE);

    var byteValue = new byte[10];
    Arrays.fill(byteValue, (byte) 10);
    document.field("bytes", byteValue);

    document.field("utf8String", "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C");
    document.field("recordId", new RecordId(10, 10));

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    c.set(Calendar.MILLISECOND, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.HOUR_OF_DAY, 0);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("name"), document.field("name"));
    assertEquals(extr.<Object>field("age"), document.field("age"));
    assertEquals(extr.<Object>field("youngAge"), document.field("youngAge"));
    assertEquals(extr.<Object>field("oldAge"), document.field("oldAge"));
    assertEquals(extr.<Object>field("heigth"), document.field("heigth"));
    assertEquals(extr.<Object>field("bitHeigth"), document.field("bitHeigth"));
    assertEquals(extr.<Object>field("class"), document.field("class"));
    // TODO fix char management issue:#2427
    // assertEquals(document.field("character"), extr.field("character"));
    assertEquals(extr.<Object>field("alive"), document.field("alive"));
    assertEquals(extr.<Object>field("dateTime"), document.field("dateTime"));
    assertEquals(extr.field("date"), c.getTime());
    assertEquals(extr.field("date1"), c1.getTime());
    //    assertEquals(extr.<String>field("bytes"), document.field("bytes"));
    Assertions.assertThat(extr.<Object>field("bytes")).isEqualTo(document.field("bytes"));
    assertEquals(extr.<String>field("utf8String"), document.field("utf8String"));
    assertEquals(extr.<Object>field("recordId"), document.field("recordId"));
    assertEquals(extr.<Object>field("bigNumber"), document.field("bigNumber"));
    assertNull(extr.field("nullField"));
    // assertEquals(extr.field("ridBag"), document.field("ridBag"));

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralArray() {
    var document = (EntityImpl) session.newEntity();
    var strings = new String[3];
    strings[0] = "a";
    strings[1] = "b";
    strings[2] = "c";
    document.field("listStrings", strings);

    var shorts = new Short[3];
    shorts[0] = (short) 1;
    shorts[1] = (short) 2;
    shorts[2] = (short) 3;
    document.field("shorts", shorts);

    var longs = new Long[3];
    longs[0] = (long) 1;
    longs[1] = (long) 2;
    longs[2] = (long) 3;
    document.field("longs", longs);

    var ints = new Integer[3];
    ints[0] = 1;
    ints[1] = 2;
    ints[2] = 3;
    document.field("integers", ints);

    var floats = new Float[3];
    floats[0] = 1.1f;
    floats[1] = 2.2f;
    floats[2] = 3.3f;
    document.field("floats", floats);

    var doubles = new Double[3];
    doubles[0] = 1.1d;
    doubles[1] = 2.2d;
    doubles[2] = 3.3d;
    document.field("doubles", doubles);

    var dates = new Date[3];
    dates[0] = new Date();
    dates[1] = new Date();
    dates[2] = new Date();
    document.field("dates", dates);

    var bytes = new Byte[3];
    bytes[0] = (byte) 0;
    bytes[1] = (byte) 1;
    bytes[2] = (byte) 3;
    document.field("bytes", bytes);

    // TODO: char not currently supported
    var chars = new Character[3];
    chars[0] = 'A';
    chars[1] = 'B';
    chars[2] = 'C';
    // document.field("chars", chars);

    var booleans = new Boolean[3];
    booleans[0] = true;
    booleans[1] = false;
    booleans[2] = false;
    document.field("booleans", booleans);

    var arrayNulls = new Object[3];
    // document.field("arrayNulls", arrayNulls);

    // Object[] listMixed = new ArrayList[9];
    // listMixed[0] = new Boolean(true);
    // listMixed[1] = 1;
    // listMixed[2] = (long) 5;
    // listMixed[3] = (short) 2;
    // listMixed[4] = 4.0f;
    // listMixed[5] = 7.0D;
    // listMixed[6] = "hello";
    // listMixed[7] = new Date();
    // listMixed[8] = (byte) 10;
    // document.field("listMixed", listMixed);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(((List) extr.field("listStrings")).toArray(), document.field("listStrings"));
    assertEquals(((List) extr.field("integers")).toArray(), document.field("integers"));
    assertEquals(((List) extr.field("doubles")).toArray(), document.field("doubles"));
    assertEquals(((List) extr.field("dates")).toArray(), document.field("dates"));
    assertEquals(((List) extr.field("bytes")).toArray(), document.field("bytes"));
    assertEquals(((List) extr.field("booleans")).toArray(), document.field("booleans"));
    // assertEquals(((List) extr.field("arrayNulls")).toArray(), document.field("arrayNulls"));
    // assertEquals(extr.field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralList() {
    var document = (EntityImpl) session.newEntity();
    List<String> strings = new ArrayList<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    List<Float> floats = new ArrayList<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    List<Double> doubles = new ArrayList<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    List<Date> dates = new ArrayList<Date>();
    dates.add(new Date());
    dates.add(new Date());
    dates.add(new Date());
    document.field("dates", dates);

    List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

    // TODO: char not currently supported
    List<Character> chars = new ArrayList<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    List<Boolean> booleans = new ArrayList<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field("booleans", booleans);

    List listMixed = new ArrayList();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    document.field("listMixed", listMixed);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("listStrings"), document.field("listStrings"));
    assertEquals(extr.<Object>field("integers"), document.field("integers"));
    assertEquals(extr.<Object>field("doubles"), document.field("doubles"));
    assertEquals(extr.<Object>field("dates"), document.field("dates"));
    assertEquals(extr.<Object>field("bytes"), document.field("bytes"));
    assertEquals(extr.<Object>field("booleans"), document.field("booleans"));
    assertEquals(extr.<Object>field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralSet() throws InterruptedException {
    var document = (EntityImpl) session.newEntity();
    Set<String> strings = new HashSet<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    Set<Short> shorts = new HashSet<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    Set<Long> longs = new HashSet<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    Set<Integer> ints = new HashSet<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    Set<Float> floats = new HashSet<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    Set<Double> doubles = new HashSet<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    Set<Date> dates = new HashSet<Date>();
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    document.field("dates", dates);

    Set<Byte> bytes = new HashSet<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

    // TODO: char not currently supported
    Set<Character> chars = new HashSet<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    Set<Boolean> booleans = new HashSet<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field("booleans", booleans);

    Set listMixed = new HashSet();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    listMixed.add(new RecordId(10, 20));
    document.field("listMixed", listMixed);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("listStrings"), document.field("listStrings"));
    assertEquals(extr.<Object>field("integers"), document.field("integers"));
    assertEquals(extr.<Object>field("doubles"), document.field("doubles"));
    assertEquals(extr.<Object>field("dates"), document.field("dates"));
    assertEquals(extr.<Object>field("bytes"), document.field("bytes"));
    assertEquals(extr.<Object>field("booleans"), document.field("booleans"));
    assertEquals(extr.<Object>field("listMixed"), document.field("listMixed"));
  }

  @Test
  public void testLinkCollections() {
    var document = (EntityImpl) session.newEntity();
    Set<RecordId> linkSet = new HashSet<RecordId>();
    linkSet.add(new RecordId(10, 20));
    linkSet.add(new RecordId(10, 21));
    linkSet.add(new RecordId(10, 22));
    linkSet.add(new RecordId(11, 22));
    document.field("linkSet", linkSet, PropertyType.LINKSET);

    List<RecordId> linkList = new ArrayList<RecordId>();
    linkList.add(new RecordId(10, 20));
    linkList.add(new RecordId(10, 21));
    linkList.add(new RecordId(10, 22));
    linkList.add(new RecordId(11, 22));
    document.field("linkList", linkList, PropertyType.LINKLIST);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(
        ((Set<?>) extr.field("linkSet")).size(), ((Set<?>) document.field("linkSet")).size());
    assertTrue(((Set<?>) extr.field("linkSet")).containsAll(document.field("linkSet")));
    assertEquals(extr.<Object>field("linkList"), document.field("linkList"));
  }

  @Test
  public void testSimpleEmbeddedDoc() {
    var document = (EntityImpl) session.newEntity();
    var embedded = (EntityImpl) session.newEntity();
    embedded.field("name", "test");
    embedded.field("surname", "something");
    document.field("embed", embedded, PropertyType.EMBEDDED);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(document.fields(), extr.fields());
    EntityImpl emb = extr.field("embed");
    assertNotNull(emb);
    assertEquals(emb.<Object>field("name"), embedded.field("name"));
    assertEquals(emb.<Object>field("surname"), embedded.field("surname"));
  }

  @Test
  public void testSimpleMapStringLiteral() {
    var document = (EntityImpl) session.newEntity();

    Map<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    mapString.put("key1", "value1");
    document.field("mapString", mapString);

    Map<String, Integer> mapInt = new HashMap<String, Integer>();
    mapInt.put("key", 2);
    mapInt.put("key1", 3);
    document.field("mapInt", mapInt);

    Map<String, Long> mapLong = new HashMap<String, Long>();
    mapLong.put("key", 2L);
    mapLong.put("key1", 3L);
    document.field("mapLong", mapLong);

    Map<String, Short> shortMap = new HashMap<String, Short>();
    shortMap.put("key", (short) 2);
    shortMap.put("key1", (short) 3);
    document.field("shortMap", shortMap);

    Map<String, Date> dateMap = new HashMap<String, Date>();
    dateMap.put("key", new Date());
    dateMap.put("key1", new Date());
    document.field("dateMap", dateMap);

    Map<String, Float> floatMap = new HashMap<String, Float>();
    floatMap.put("key", 10f);
    floatMap.put("key1", 11f);
    document.field("floatMap", floatMap);

    Map<String, Double> doubleMap = new HashMap<String, Double>();
    doubleMap.put("key", 10d);
    doubleMap.put("key1", 11d);
    document.field("doubleMap", doubleMap);

    Map<String, Byte> bytesMap = new HashMap<String, Byte>();
    bytesMap.put("key", (byte) 10);
    bytesMap.put("key1", (byte) 11);
    document.field("bytesMap", bytesMap);

    Map<String, String> mapWithNulls = new HashMap<String, String>();
    mapWithNulls.put("key", "dddd");
    mapWithNulls.put("key1", null);
    document.field("bytesMap", mapWithNulls);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("mapString"), document.field("mapString"));
    assertEquals(extr.<Object>field("mapLong"), document.field("mapLong"));
    assertEquals(extr.<Object>field("shortMap"), document.field("shortMap"));
    assertEquals(extr.<Object>field("dateMap"), document.field("dateMap"));
    assertEquals(extr.<Object>field("doubleMap"), document.field("doubleMap"));
    assertEquals(extr.<Object>field("bytesMap"), document.field("bytesMap"));
  }

  @Test
  public void testlistOfList() {
    var document = (EntityImpl) session.newEntity();
    List<List<String>> list = new ArrayList<List<String>>();
    List<String> ls = new ArrayList<String>();
    ls.add("test1");
    ls.add("test2");
    list.add(ls);
    document.field("complexList", list);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("complexList"), document.field("complexList"));
  }

  @Test
  public void testArrayOfArray() {
    var document = (EntityImpl) session.newEntity();
    var array = new String[1][];
    var ls = new String[2];
    ls[0] = "test1";
    ls[1] = "test2";
    array[0] = ls;
    document.field("complexArray", array);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    List<List<String>> savedValue = extr.field("complexArray");
    assertEquals(savedValue.size(), array.length);
    assertEquals(savedValue.getFirst().size(), array[0].length);
    assertEquals(savedValue.getFirst().get(0), array[0][0]);
    assertEquals(savedValue.getFirst().get(1), array[0][1]);
  }

  @Test
  public void testEmbeddedListOfEmbeddedMap() {
    var document = (EntityImpl) session.newEntity();
    List<Map<String, String>> coll = new ArrayList<Map<String, String>>();
    Map<String, String> map = new HashMap<String, String>();
    map.put("first", "something");
    map.put("second", "somethingElse");
    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("first", "something");
    map2.put("second", "somethingElse");
    coll.add(map);
    coll.add(map2);
    document.field("list", coll);
    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("list"), document.field("list"));
  }

  @Test
  public void testMapOfEmbeddedDocument() {
    var document = (EntityImpl) session.newEntity();

    var embeddedInMap = (EntityImpl) session.newEntity();
    embeddedInMap.field("name", "test");
    embeddedInMap.field("surname", "something");
    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    map.put("embedded", embeddedInMap);
    document.field("map", map, PropertyType.EMBEDDEDMAP);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    Map<String, EntityImpl> mapS = extr.field("map");
    assertEquals(1, mapS.size());
    var emb = mapS.get("embedded");
    assertNotNull(emb);
    assertEquals(emb.<Object>field("name"), embeddedInMap.field("name"));
    assertEquals(emb.<Object>field("surname"), embeddedInMap.field("surname"));
  }

  @Test
  public void testMapOfLink() {
    // needs a database because of the lazy loading
    var document = (EntityImpl) session.newEntity();

    Map<String, Identifiable> map = new HashMap<String, Identifiable>();
    map.put("link", new RecordId(0, 0));
    document.field("map", map, PropertyType.LINKMAP);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("map"), document.field("map"));
  }

  @Test
  public void testDocumentSimple() {
    var document = (EntityImpl) session.newEntity("TestClass");
    document.field("test", "test");
    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    //      assertEquals(extr.getClassName(), document.getClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("test"), document.field("test"));
  }

  @Test
  public void testDocumentWithCostum() {
    var old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);

    var document = (EntityImpl) session.newEntity();
    document.field("test", "test");
    document.field("custom", new DocumentSchemalessBinarySerializationTest.Custom());

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.getSchemaClassName(), document.getSchemaClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("test"), document.field("test"));
    assertEquals(extr.<Object>field("custom"), document.field("custom"));
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test
  public void testDocumentWithCostumDocument() {
    var document = (EntityImpl) session.newEntity();
    document.field("test", "test");
    document.field("custom", new DocumentSchemalessBinarySerializationTest.CustomDocument());

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.getSchemaClassName(), document.getSchemaClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("test"), document.field("test"));
    assertEquals(extr.<Object>field("custom"), document.field("custom"));
  }

  @Test(expected = SerializationException.class)
  public void testSetOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    Set<Object> embeddedSet = new HashSet<Object>();
    embeddedSet.add(new WrongData());
    document.field("embeddedSet", embeddedSet, PropertyType.EMBEDDEDSET);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = SerializationException.class)
  public void testListOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    List<Object> embeddedList = new ArrayList<Object>();
    embeddedList.add(new WrongData());
    document.field("embeddedList", embeddedList, PropertyType.EMBEDDEDLIST);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = SerializationException.class)
  public void testMapOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    Map<String, Object> embeddedMap = new HashMap<String, Object>();
    embeddedMap.put("name", new WrongData());
    document.field("embeddedMap", embeddedMap, PropertyType.EMBEDDEDMAP);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkSetOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    Set<Object> linkSet = new HashSet<Object>();
    linkSet.add(new WrongData());
    document.field("linkSet", linkSet, PropertyType.LINKSET);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkListOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    List<Object> linkList = new ArrayList<Object>();
    linkList.add(new WrongData());
    document.field("linkList", linkList, PropertyType.LINKLIST);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = ClassCastException.class)
  public void testLinkMapOfWrongData() {
    var document = (EntityImpl) session.newEntity();

    Map<String, Object> linkMap = new HashMap<String, Object>();
    linkMap.put("name", new WrongData());
    document.field("linkMap", linkMap, PropertyType.LINKMAP);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test(expected = SerializationException.class)
  public void testFieldWrongData() {
    var document = (EntityImpl) session.newEntity();

    document.field("wrongData", new WrongData());

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
  }

  @Test
  public void testCollectionOfEmbeddedDocument() {
    var document = (EntityImpl) session.newEntity();

    var embeddedInList = (EntityImpl) session.newEntity();
    embeddedInList.field("name", "test");
    embeddedInList.field("surname", "something");

    var embeddedInList2 = (EntityImpl) session.newEntity();
    embeddedInList2.field("name", "test1");
    embeddedInList2.field("surname", "something2");

    List<EntityImpl> embeddedList = new ArrayList<EntityImpl>();
    embeddedList.add(embeddedInList);
    embeddedList.add(embeddedInList2);
    embeddedList.add(null);
    embeddedList.add((EntityImpl) session.newEntity());
    document.field("embeddedList", embeddedList, PropertyType.EMBEDDEDLIST);

    var embeddedInSet = (EntityImpl) session.newEntity();
    embeddedInSet.field("name", "test2");
    embeddedInSet.field("surname", "something3");

    var embeddedInSet2 = (EntityImpl) session.newEntity();
    embeddedInSet2.field("name", "test5");
    embeddedInSet2.field("surname", "something6");

    Set<EntityImpl> embeddedSet = new HashSet<EntityImpl>();
    embeddedSet.add(embeddedInSet);
    embeddedSet.add(embeddedInSet2);
    embeddedSet.add((EntityImpl) session.newEntity());
    document.field("embeddedSet", embeddedSet, PropertyType.EMBEDDEDSET);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    List<EntityImpl> ser = extr.field("embeddedList");
    assertEquals(4, ser.size());
    assertNotNull(ser.get(0));
    assertNotNull(ser.get(1));
    assertNull(ser.get(2));
    assertNotNull(ser.get(3));
    var inList = ser.get(0);
    assertNotNull(inList);
    assertEquals(inList.<Object>field("name"), embeddedInList.field("name"));
    assertEquals(inList.<Object>field("surname"), embeddedInList.field("surname"));

    Set<EntityImpl> setEmb = extr.field("embeddedSet");
    assertEquals(3, setEmb.size());
    var ok = false;
    for (var inSet : setEmb) {
      assertNotNull(inSet);
      if (embeddedInSet.field("name").equals(inSet.field("name"))
          && embeddedInSet.field("surname").equals(inSet.field("surname"))) {
        if (true) {
          if (true) {
            if (true) {
              ok = true;
            }
          }
        }
      }
    }
    assertTrue("not found record in the set after serilize", ok);
  }

  @Test
  public void testSerializableValue() {
    var old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);

    var document = (EntityImpl) session.newEntity();
    var ser = new SimpleSerializableClass();
    ser.name = "testName";
    document.field("seri", ser);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertNotNull(extr.field("seri"));
    assertEquals(PropertyType.CUSTOM, extr.getPropertyType("seri"));
    SimpleSerializableClass newser = extr.field("seri");
    assertEquals(newser.name, ser.name);
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test
  public void testFieldNames() {
    var document = (EntityImpl) session.newEntity();
    document.fields("a", 1, "b", 2, "c", 3);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    final var fields = extr.fieldNames();

    assertNotNull(fields);
    assertEquals(3, fields.length);
    assertEquals("a", fields[0]);
    assertEquals("b", fields[1]);
    assertEquals("c", fields[2]);
  }

  @Test
  public void testWithRemove() {
    var document = (EntityImpl) session.newEntity();
    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.removeField("oldAge");

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(document.field("name"), extr.<Object>field("name"));
    assertEquals(document.<Object>field("age"), extr.field("age"));
    assertEquals(document.<Object>field("youngAge"), extr.field("youngAge"));
    assertNull(extr.field("oldAge"));
  }

  @Test
  public void testListOfMapsWithNull() {
    var document = (EntityImpl) session.newEntity();

    List lista = new ArrayList<>();
    Map mappa = new LinkedHashMap<>();
    mappa.put("prop1", "val1");
    mappa.put("prop2", null);
    lista.add(mappa);

    mappa = new HashMap();
    mappa.put("prop", "val");
    lista.add(mappa);
    document.setProperty("list", lista);

    var serializerDelta = DocumentSerializerDelta.instance();
    var res = serializerDelta.serialize(session, document);
    var extr = (EntityImpl) session.newEntity();
    serializerDelta.deserialize(session, res, extr);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.<Object>field("list"), document.field("list"));
  }

  public static class CustomDocument implements EntitySerializable {

    private EntityImpl document;

    @Override
    public void fromDocument(EntityImpl document) {
      this.document = document;
    }

    @Override
    public EntityImpl toEntity(DatabaseSessionInternal db) {
      document = (EntityImpl) db.newEntity();
      document.field("test", "some strange content");
      return document;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      return document.field("test").equals(((CustomDocument) obj).document.field("test"));
    }
  }

  private static class WrongData {

  }
}
