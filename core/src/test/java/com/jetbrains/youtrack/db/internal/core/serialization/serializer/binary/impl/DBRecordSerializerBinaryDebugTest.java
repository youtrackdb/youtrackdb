package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinaryDebug;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DBRecordSerializerBinaryDebugTest extends DbTestBase {

  private RecordSerializer previous;

  @Before
  public void before() {
    previous = DatabaseSessionAbstract.getDefaultSerializer();
    DatabaseSessionAbstract.setDefaultSerializer(new RecordSerializerBinary());
  }

  @After
  public void after() {
    DatabaseSessionAbstract.setDefaultSerializer(previous);
  }

  @Test
  public void testSimpleDocumentDebug() {

    var doc = (EntityImpl) session.newEntity();
    doc.field("test", "test");
    doc.field("anInt", 2);
    doc.field("anDouble", 2D);

    var bytes = doc.toStream();

    var debugger = new RecordSerializerBinaryDebug();
    var debug = debugger.deserializeDebug(bytes, session);

    assertEquals(debug.properties.size(), 3);
    assertEquals(debug.properties.get(0).name, "test");
    assertEquals(debug.properties.get(0).type, PropertyType.STRING);
    assertEquals(debug.properties.get(0).value, "test");

    assertEquals(debug.properties.get(1).name, "anInt");
    assertEquals(debug.properties.get(1).type, PropertyType.INTEGER);
    assertEquals(debug.properties.get(1).value, 2);

    assertEquals(debug.properties.get(2).name, "anDouble");
    assertEquals(debug.properties.get(2).type, PropertyType.DOUBLE);
    assertEquals(debug.properties.get(2).value, 2D);
  }

  @Test
  public void testSchemaFullDocumentDebug() {
    var clazz = session.getMetadata().getSchema().createClass("some");
    clazz.createProperty(session, "testP", PropertyType.STRING);
    clazz.createProperty(session, "theInt", PropertyType.INTEGER);
    var doc = (EntityImpl) session.newEntity("some");
    doc.field("testP", "test");
    doc.field("theInt", 2);
    doc.field("anDouble", 2D);

    var bytes = doc.toStream();

    var debugger = new RecordSerializerBinaryDebug();
    var debug = debugger.deserializeDebug(bytes, session);

    assertEquals(debug.properties.size(), 3);
    assertEquals(debug.properties.get(0).name, "testP");
    assertEquals(debug.properties.get(0).type, PropertyType.STRING);
    assertEquals(debug.properties.get(0).value, "test");

    assertEquals(debug.properties.get(1).name, "theInt");
    assertEquals(debug.properties.get(1).type, PropertyType.INTEGER);
    assertEquals(debug.properties.get(1).value, 2);

    assertEquals(debug.properties.get(2).name, "anDouble");
    assertEquals(debug.properties.get(2).type, PropertyType.DOUBLE);
    assertEquals(debug.properties.get(2).value, 2D);
  }

  @Test
  public void testSimpleBrokenDocumentDebug() {
    var doc = (EntityImpl) session.newEntity();
    doc.field("test", "test");
    doc.field("anInt", 2);
    doc.field("anDouble", 2D);

    var bytes = doc.toStream();
    var brokenBytes = new byte[bytes.length - 10];
    System.arraycopy(bytes, 0, brokenBytes, 0, bytes.length - 10);

    var debugger = new RecordSerializerBinaryDebug();
    var debug = debugger.deserializeDebug(brokenBytes, session);

    assertEquals(debug.properties.size(), 3);
    assertEquals(debug.properties.get(0).name, "test");
    assertEquals(debug.properties.get(0).type, PropertyType.STRING);
    assertTrue(debug.properties.get(0).faildToRead);
    assertNotNull(debug.properties.get(0).readingException);

    assertEquals(debug.properties.get(1).name, "anInt");
    assertEquals(debug.properties.get(1).type, PropertyType.INTEGER);
    assertTrue(debug.properties.get(1).faildToRead);
    assertNotNull(debug.properties.get(1).readingException);

    assertEquals(debug.properties.get(2).name, "anDouble");
    assertEquals(debug.properties.get(2).type, PropertyType.DOUBLE);
    assertTrue(debug.properties.get(2).faildToRead);
    assertNotNull(debug.properties.get(2).readingException);
  }

  @Test
  public void testBrokenSchemaFullDocumentDebug() {
    var clazz = session.getMetadata().getSchema().createClass("some");
    clazz.createProperty(session, "testP", PropertyType.STRING);
    clazz.createProperty(session, "theInt", PropertyType.INTEGER);
    var doc = (EntityImpl) session.newEntity("some");
    doc.field("testP", "test");
    doc.field("theInt", 2);
    doc.field("anDouble", 2D);

    var bytes = doc.toStream();
    var brokenBytes = new byte[bytes.length - 10];
    System.arraycopy(bytes, 0, brokenBytes, 0, bytes.length - 10);

    var debugger = new RecordSerializerBinaryDebug();
    var debug = debugger.deserializeDebug(brokenBytes, session);

    assertEquals(debug.properties.size(), 3);
    assertEquals(debug.properties.get(0).name, "testP");
    assertEquals(debug.properties.get(0).type, PropertyType.STRING);
    assertTrue(debug.properties.get(0).faildToRead);
    assertNotNull(debug.properties.get(0).readingException);

    assertEquals(debug.properties.get(1).name, "theInt");
    assertEquals(debug.properties.get(1).type, PropertyType.INTEGER);
    assertTrue(debug.properties.get(1).faildToRead);
    assertNotNull(debug.properties.get(1).readingException);

    assertEquals(debug.properties.get(2).name, "anDouble");
    assertEquals(debug.properties.get(2).type, PropertyType.DOUBLE);
    assertTrue(debug.properties.get(2).faildToRead);
    assertNotNull(debug.properties.get(2).readingException);
  }
}
