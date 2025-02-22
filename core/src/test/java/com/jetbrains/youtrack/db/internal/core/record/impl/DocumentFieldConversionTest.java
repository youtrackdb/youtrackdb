package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
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
    session.begin();
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
    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionInteger() {
    session.begin();
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

    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionString() {
    session.begin();
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
    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionFloat() {
    session.begin();
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

    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionDouble() {
    session.begin();
    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("double", 1);
    assertTrue(doc.field("double") instanceof Double);

    assertThat(doc.<Double>field("double")).isEqualTo(1d);

    doc.field("double", 2L);
    assertTrue(doc.field("double") instanceof Double);

    assertThat(doc.<Double>field("double")).isEqualTo(2d);

    doc.field("double", "3");
    assertTrue(doc.field("double") instanceof Double);

    assertThat(doc.<Double>field("double")).isEqualTo(3d);

    doc.field("double", 4f);
    assertTrue(doc.field("double") instanceof Double);

    assertThat(doc.<Double>field("double")).isEqualTo(4d);

    doc.field("double", new BigDecimal("6"));
    assertTrue(doc.field("double") instanceof Double);

    assertThat(doc.<Double>field("double")).isEqualTo(6d);

    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionLong() {
    session.begin();
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
    assertThat(doc.<Long>field("long")).isEqualTo(6L);

    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionBoolean() {
    session.begin();
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
    session.rollback();
  }

  @Test
  public void testLiteralToSchemaConversionDecimal() {
    session.begin();
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
    session.rollback();
  }

  @Test
  public void testConversionAlsoWithWrongType() {
    session.begin();
    var doc = (EntityImpl) session.newEntity(clazz);

    doc.field("float", 2, PropertyType.INTEGER);
    assertTrue(doc.field("float") instanceof Float);
    assertThat(doc.<Float>field("float")).isEqualTo(2f);

    doc.field("integer", 3f, PropertyType.FLOAT);
    assertTrue(doc.field("integer") instanceof Integer);
    assertThat(doc.<Integer>field("integer")).isEqualTo(3);

    doc.field("double", 1L, PropertyType.LONG);
    assertTrue(doc.field("double") instanceof Double);
    assertThat(doc.<Double>field("double")).isEqualTo(1d);

    doc.field("long", 1d, PropertyType.DOUBLE);
    assertTrue(doc.field("long") instanceof Long);

    assertThat(doc.<Long>field("long")).isEqualTo(1L);
    session.rollback();
  }

  @Test
  public void testLiteralConversionAfterSchemaSet() {
    session.begin();
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
    session.rollback();
  }
}
