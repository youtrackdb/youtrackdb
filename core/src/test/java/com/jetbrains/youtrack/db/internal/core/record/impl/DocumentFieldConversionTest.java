package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class DocumentFieldConversionTest extends BaseMemoryInternalDatabase {

  private SchemaClass clazz;

  public void beforeTest() throws Exception {
    super.beforeTest();
    clazz = session.getMetadata().getSchema().createClass("testClass");
    clazz.createProperty(session, "integer", PropertyType.INTEGER);
    clazz.createProperty(session, "string", PropertyType.STRING);
    clazz.createProperty(session, "boolean", PropertyType.BOOLEAN);
    clazz.createProperty(session, "long", PropertyType.LONG);
    clazz.createProperty(session, "float", PropertyType.FLOAT);
    clazz.createProperty(session, "double", PropertyType.DOUBLE);
    clazz.createProperty(session, "decimal", PropertyType.DECIMAL);
    clazz.createProperty(session, "date", PropertyType.DATE);

    clazz.createProperty(session, "byteList", PropertyType.EMBEDDEDLIST, PropertyType.BYTE);
    clazz.createProperty(session, "integerList", PropertyType.EMBEDDEDLIST, PropertyType.INTEGER);
    clazz.createProperty(session, "longList", PropertyType.EMBEDDEDLIST, PropertyType.LONG);
    clazz.createProperty(session, "stringList", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(session, "floatList", PropertyType.EMBEDDEDLIST, PropertyType.FLOAT);
    clazz.createProperty(session, "doubleList", PropertyType.EMBEDDEDLIST, PropertyType.DOUBLE);
    clazz.createProperty(session, "decimalList", PropertyType.EMBEDDEDLIST, PropertyType.DECIMAL);
    clazz.createProperty(session, "booleanList", PropertyType.EMBEDDEDLIST, PropertyType.BOOLEAN);
    clazz.createProperty(session, "dateList", PropertyType.EMBEDDEDLIST, PropertyType.DATE);

    clazz.createProperty(session, "byteSet", PropertyType.EMBEDDEDSET, PropertyType.BYTE);
    clazz.createProperty(session, "integerSet", PropertyType.EMBEDDEDSET, PropertyType.INTEGER);
    clazz.createProperty(session, "longSet", PropertyType.EMBEDDEDSET, PropertyType.LONG);
    clazz.createProperty(session, "stringSet", PropertyType.EMBEDDEDSET, PropertyType.STRING);
    clazz.createProperty(session, "floatSet", PropertyType.EMBEDDEDSET, PropertyType.FLOAT);
    clazz.createProperty(session, "doubleSet", PropertyType.EMBEDDEDSET, PropertyType.DOUBLE);
    clazz.createProperty(session, "decimalSet", PropertyType.EMBEDDEDSET, PropertyType.DECIMAL);
    clazz.createProperty(session, "booleanSet", PropertyType.EMBEDDEDSET, PropertyType.BOOLEAN);
    clazz.createProperty(session, "dateSet", PropertyType.EMBEDDEDSET, PropertyType.DATE);

    clazz.createProperty(session, "byteMap", PropertyType.EMBEDDEDMAP, PropertyType.BYTE);
    clazz.createProperty(session, "integerMap", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);
    clazz.createProperty(session, "longMap", PropertyType.EMBEDDEDMAP, PropertyType.LONG);
    clazz.createProperty(session, "stringMap", PropertyType.EMBEDDEDMAP, PropertyType.STRING);
    clazz.createProperty(session, "floatMap", PropertyType.EMBEDDEDMAP, PropertyType.FLOAT);
    clazz.createProperty(session, "doubleMap", PropertyType.EMBEDDEDMAP, PropertyType.DOUBLE);
    clazz.createProperty(session, "decimalMap", PropertyType.EMBEDDEDMAP, PropertyType.DECIMAL);
    clazz.createProperty(session, "booleanMap", PropertyType.EMBEDDEDMAP, PropertyType.BOOLEAN);
    clazz.createProperty(session, "dateMap", PropertyType.EMBEDDEDMAP, PropertyType.DATE);
  }

  @Test
  public void testDateToSchemaConversion() {
    var calendare = Calendar.getInstance();
    calendare.set(Calendar.MILLISECOND, 0);
    var date = calendare.getTime();

    var dateString = session.getStorage().getConfiguration().getDateTimeFormatInstance()
        .format(date);
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("date", dateString);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(date, doc.field("date"));

    doc.field("date", 20304);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());

    doc.field("date", 43432440f);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(43432440L, ((Date) doc.field("date")).getTime());

    doc.field("date", 43432444D);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(43432444L, ((Date) doc.field("date")).getTime());

    doc.field("date", 20304L);
    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());
  }

  @Test
  public void testLiteralToSchemaConversionInteger() {
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("integer", 2L);
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(2, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(2);

    doc.field("integer", 3f);
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(3, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(3);

    doc.field("integer", 4d);
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(4, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(4);

    doc.field("integer", "5");
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(5, doc.field("integer"));
    assertThat(doc.<Integer>field("integer")).isEqualTo(5);

    doc.field("integer", new BigDecimal("6"));
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(6, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(6);

    // doc.field("integer", true);
    // assertTrue(doc.field("integer") instanceof Integer);
    // assertEquals(1, doc.field("integer"));

  }

  @Test
  public void testLiteralToSchemaConversionString() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("string", 1);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("1", doc.field("string"));

    doc.field("string", 2L);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("2", doc.field("string"));

    doc.field("string", 3f);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("3.0", doc.field("string"));

    doc.field("string", 4d);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("4.0", doc.field("string"));

    doc.field("string", new BigDecimal("6"));
    assertTrue(doc.field("string") instanceof String);
    assertEquals("6", doc.field("string"));

    doc.field("string", true);
    assertTrue(doc.field("string") instanceof String);
    assertEquals("true", doc.field("string"));
  }

  @Test
  public void testLiteralToSchemaConversionFloat() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("float", 1);
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(1f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(1f);

    doc.field("float", 2L);
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(2f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(2f);

    doc.field("float", "3");
    assertTrue(doc.field("float") instanceof Float);

    assertThat(doc.<Float>field("float")).isEqualTo(3f);

    doc.field("float", 4d);
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(4f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(4f);

    doc.field("float", new BigDecimal("6"));
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(6f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(6f);

    // doc.field("float", true);
    // assertTrue(doc.field("float") instanceof Float);
    // assertEquals(1f, doc.field("float"));

  }

  @Test
  public void testLiteralToSchemaConversionDouble() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("double", 1);
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(1d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(1d);

    doc.field("double", 2L);
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(2d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(2d);

    doc.field("double", "3");
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(3d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(3d);

    doc.field("double", 4f);
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(4d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(4d);

    doc.field("double", new BigDecimal("6"));
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(6d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(6d);

    // doc.field("double", true);
    // assertTrue(doc.field("double") instanceof Double);
    // assertEquals(1d, doc.field("double"));

  }

  @Test
  public void testLiteralToSchemaConversionLong() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("long", 1);
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(1L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(1L);

    doc.field("long", 2f);
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(2L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(2L);

    doc.field("long", "3");
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(3L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(3L);

    doc.field("long", 4d);
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(4L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(4L);

    doc.field("long", new BigDecimal("6"));
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(6L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(6L);

    // doc.field("long", true);
    // assertTrue(doc.field("long") instanceof Long);
    // assertEquals(1, doc.field("long"));

  }

  @Test
  public void testLiteralToSchemaConversionBoolean() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("boolean", 0);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(false, doc.field("boolean"));

    doc.field("boolean", 1L);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", 2f);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", "true");
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", 4d);
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    doc.field("boolean", new BigDecimal("6"));
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));
  }

  @Test
  public void testLiteralToSchemaConversionDecimal() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("decimal", 0);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(BigDecimal.ZERO, doc.field("decimal"));

    doc.field("decimal", 1L);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(BigDecimal.ONE, doc.field("decimal"));

    doc.field("decimal", 2f);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("2.0"), doc.field("decimal"));

    doc.field("decimal", "3");
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("3"), doc.field("decimal"));

    doc.field("decimal", 4d);
    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal("4.0"), doc.field("decimal"));

    doc.field("boolean", new BigDecimal("6"));
    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));
  }

  @Test
  public void testConversionAlsoWithWrongType() {

    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("float", 2, PropertyType.INTEGER);
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(2f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(2f);

    doc.field("integer", 3f, PropertyType.FLOAT);
    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(3, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(3);

    doc.field("double", 1L, PropertyType.LONG);
    assertTrue(doc.field("double") instanceof Double);
    //    assertEquals(1d, doc.field("double"));

    assertThat(doc.<Double>field("double")).isEqualTo(1d);

    doc.field("long", 1d, PropertyType.DOUBLE);
    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(1L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(1L);
  }

  @Test
  public void testLiteralConversionAfterSchemaSet() {

    var doc = (EntityImpl) session.newEntity();

    doc.field("float", 1);
    doc.field("integer", 3f);
    doc.field("double", 2L);
    doc.field("long", 2D);
    doc.field("string", 25);
    doc.field("boolean", "true");
    doc.field("decimal", -1);
    doc.field("date", 20304L);

    doc.setClass(clazz);
    assertTrue(doc.field("float") instanceof Float);
    //    assertEquals(1f, doc.field("float"));

    assertThat(doc.<Float>field("float")).isEqualTo(1f);

    assertTrue(doc.field("integer") instanceof Integer);
    //    assertEquals(3, doc.field("integer"));

    assertThat(doc.<Integer>field("integer")).isEqualTo(3);

    assertTrue(doc.field("long") instanceof Long);
    //    assertEquals(2L, doc.field("long"));

    assertThat(doc.<Long>field("long")).isEqualTo(2L);

    assertTrue(doc.field("string") instanceof String);
    assertEquals("25", doc.field("string"));

    assertTrue(doc.field("boolean") instanceof Boolean);
    assertEquals(true, doc.field("boolean"));

    assertTrue(doc.field("decimal") instanceof BigDecimal);
    assertEquals(new BigDecimal(-1), doc.field("decimal"));

    assertTrue(doc.field("date") instanceof Date);
    assertEquals(20304L, ((Date) doc.field("date")).getTime());
  }

  @SuppressWarnings({"rawtypes"})
  @Test
  public void testListByteCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("byteList", values);
    fillList(values);

    Set set = new HashSet();
    doc.field("byteSet", set);
    fillSet(set);
    doc.autoConvertValues();

    values = doc.field("byteList");
    for (var val : values) {
      assertTrue(val instanceof Byte);
      assertEquals(val, (byte) 1);
    }
    set = doc.field("byteSet");
    for (var val : set) {
      assertTrue(val instanceof Byte);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains((byte) i));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testCollectionIntegerCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("integerList", values);
    fillList(values);
    Set set = new HashSet();
    doc.field("integerSet", set);
    fillSet(set);

    doc.autoConvertValues();

    values = doc.field("integerList");
    for (var val : values) {
      assertTrue(val instanceof Integer);
      assertEquals(1, val);
    }
    set = doc.field("integerSet");
    for (var val : set) {
      assertTrue(val instanceof Integer);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains(i));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testCollectionLongCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("longList", values);
    fillList(values);
    Set set = new HashSet();
    doc.field("longSet", set);
    fillSet(set);

    doc.autoConvertValues();

    values = doc.field("longList");
    for (var val : values) {
      assertTrue(val instanceof Long);
      assertEquals(1L, val);
    }
    set = doc.field("longSet");
    for (var val : set) {
      assertTrue(val instanceof Long);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains((long) i));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testCollectionBooleanCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("booleanList", values);
    values.add((byte) 1);
    values.add("true");
    values.add(1L);
    values.add(1);
    values.add((short) 1);

    values.add(1f);
    values.add(1d);
    values.add(BigDecimal.ONE);

    Set set = new HashSet();
    doc.field("booleanSet", set);
    set.add((byte) 1);
    set.add(2L);
    set.add(3);
    set.add((short) 4);
    set.add("true");
    set.add(6f);
    set.add(7d);
    set.add(new BigDecimal(8));

    doc.autoConvertValues();

    values = doc.field("booleanList");
    for (var val : values) {
      assertTrue(val instanceof Boolean);
      assertEquals(true, val);
    }

    set = doc.field("booleanSet");
    assertEquals(1, set.size());
    assertTrue(set.iterator().next() instanceof Boolean);
    assertTrue((Boolean) set.iterator().next());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testCollectionStringCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("stringList", values);
    values.add((byte) 1);
    values.add(1L);
    values.add(1);
    values.add((short) 1);

    values.add(1f);
    values.add(1d);
    values.add(BigDecimal.ONE);

    Set set = new HashSet();
    doc.field("stringSet", set);
    fillSet(set);
    doc.autoConvertValues();

    values = doc.field("stringList");
    for (var val : values) {
      assertTrue(val instanceof String);
      assertTrue(((String) val).contains("1"));
    }
    set = doc.field("stringSet");
    for (var val : set) {
      assertTrue(val instanceof String);
    }
    for (var i = 1; i < 7; i++) {
      var contain = false;
      for (var object : set) {
        if (object.toString().contains(((Integer) i).toString())) {
          contain = true;
        }
      }
      assertTrue(contain);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void fillSet(Set set) {
    set.add((byte) 1);
    set.add(2L);
    set.add(3);
    set.add((short) 4);
    set.add("5");
    set.add(6f);
    set.add(7d);
    set.add(new BigDecimal(8));
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testCollectionFloatCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("floatList", values);
    fillList(values);

    Set set = new HashSet();
    doc.field("floatSet", set);
    fillSet(set);

    doc.autoConvertValues();

    values = doc.field("floatList");
    for (var val : values) {
      assertTrue(val instanceof Float);
      assertEquals(1f, val);
    }
    set = doc.field("floatSet");
    for (var val : set) {
      assertTrue(val instanceof Float);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains(Float.valueOf(i)));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void fillList(List values) {
    values.add((byte) 1);
    values.add(1L);
    values.add(1);
    values.add((short) 1);
    values.add("1");
    values.add(1f);
    values.add(1d);
    values.add(BigDecimal.ONE);
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testCollectionDoubleCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("doubleList", values);
    fillList(values);

    Set set = new HashSet();
    doc.field("doubleSet", set);
    fillSet(set);

    doc.autoConvertValues();

    values = doc.field("doubleList");
    for (var val : values) {
      assertTrue(val instanceof Double);
      assertEquals(1d, val);
    }
    set = doc.field("doubleSet");
    for (var val : set) {
      assertTrue(val instanceof Double);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains(Double.valueOf(i)));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testCollectionDecimalCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("decimalList", values);
    fillList(values);

    Set set = new HashSet();
    doc.field("decimalSet", set);
    fillSet(set);
    doc.autoConvertValues();

    values = doc.field("decimalList");
    for (var val : values) {
      assertTrue(val instanceof BigDecimal);
      assertTrue(val.toString().contains("1"));
    }

    set = doc.field("decimalSet");
    for (var val : set) {
      assertTrue(val instanceof BigDecimal);
    }
    for (var i = 1; i < 7; i++) {
      var contain = false;
      for (var object : set) {
        if (object.toString().contains(((Integer) i).toString())) {
          contain = true;
        }
      }
      assertTrue(contain);
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testCollectionDateCoversion() {

    List values = new ArrayList();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("dateList", values);
    values.add(1L);
    values.add(1);
    values.add((short) 1);
    values.add(1f);
    values.add(1d);
    values.add(BigDecimal.ONE);
    Set set = new HashSet();
    doc.field("dateSet", set);
    set.add(1L);
    set.add(2);
    set.add((short) 3);
    set.add(4f);
    set.add(5d);
    set.add(new BigDecimal(6));

    doc.autoConvertValues();

    values = doc.field("dateList");
    for (var val : values) {
      assertTrue(val instanceof Date);
      assertEquals(1, ((Date) val).getTime());
    }
    set = doc.field("dateSet");
    for (var val : set) {
      assertTrue(val instanceof Date);
    }
    for (var i = 1; i < 7; i++) {
      assertTrue(set.contains(new Date(i)));
    }
  }

  @Test
  public void testMapIntegerConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("integerMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("integerMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Integer);
      assertEquals(1, val);
    }
  }

  @Test
  public void testMapLongConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("longMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("longMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Long);
      assertEquals(1L, val);
    }
  }

  @Test
  public void testMapByteConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("byteMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("byteMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Byte);
      assertEquals((byte) 1, val);
    }
  }

  @Test
  public void testMapFloatConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("floatMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("floatMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Float);
      assertEquals(1f, val);
    }
  }

  @Test
  public void testMapDoubleConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("doubleMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("doubleMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Double);
      assertEquals(1d, val);
    }
  }

  @Test
  public void testMapDecimalConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("decimalMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("decimalMap");
    for (var val : values.values()) {
      assertTrue(val instanceof BigDecimal);
      assertTrue(val.toString().contains("1"));
    }
  }

  @Test
  public void testMapStringConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("stringMap", values);
    fillMap(values);
    doc.autoConvertValues();
    values = doc.field("stringMap");
    for (var val : values.values()) {
      assertTrue(val instanceof String);
      assertTrue(val.toString().contains("1"));
    }
  }

  @Test
  public void testMapDateConversion() {

    Map<String, Object> values = new HashMap<String, Object>();
    var doc = (EntityImpl) session.newEntity(clazz);
    doc.field("dateMap", values);
    values.put("first", (byte) 1);
    values.put("second", (short) 1);
    values.put("third", 1);
    values.put("forth", 1L);
    values.put("fifth", 1F);
    values.put("sixth", 1D);
    values.put("eighth", BigDecimal.ONE);
    doc.autoConvertValues();
    values = doc.field("dateMap");
    for (var val : values.values()) {
      assertTrue(val instanceof Date);
      assertEquals(1, ((Date) val).getTime());
    }
  }

  private void fillMap(Map<String, Object> values) {
    values.put("first", (byte) 1);
    values.put("second", (short) 1);
    values.put("third", 1);
    values.put("forth", 1L);
    values.put("fifth", 1F);
    values.put("sixth", 1D);
    values.put("seventh", "1");
    values.put("eighth", BigDecimal.ONE);
  }
}
