package com.orientechnologies.orient.core.record;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

/**
 *
 */
public class DocumentIndependentJavaSerializationTest extends BaseMemoryDatabase {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    byte[] ser;
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "test", OType.STRING);
    ODocument doc = new ODocument(clazz);
    doc.field("test", "Some Value");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);
    ser = baos.toByteArray();

    ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
    ODocument newDoc = (ODocument) input.readObject();

    assertEquals("Test", newDoc.getClassName());
    assertEquals("Some Value", newDoc.field("test"));
  }

  @Test
  public void testDeserializationSave() throws IOException, ClassNotFoundException {
    ODocument doc = new ODocument("Test");
    doc.field("test", "Some Value");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);
    byte[] ser = baos.toByteArray();

    OClass clazz = db.getMetadata().getSchema().getClass("Test");
    clazz.createProperty(db, "test", OType.STRING);
    ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
    ODocument doc1 = (ODocument) input.readObject();
    assertEquals(doc1.recordFormat, db.getSerializer());
    assertEquals("Test", doc1.getClassName());
    assertEquals("Some Value", doc1.field("test"));
  }
}
