package com.orientechnologies.orient.core.record;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
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
    try (OxygenDB ctx = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + DocumentIndependentJavaSerializationTest.class.getSimpleName()
              + " memory users (admin identified by 'adminpwd' role admin)");
      try (var db =
          ctx.open(
              DocumentIndependentJavaSerializationTest.class.getSimpleName(),
              "admin",
              "adminpwd")) {
        OClass clazz = db.getMetadata().getSchema().createClass("Test");
        clazz.createProperty(db, "test", OType.STRING);
        ODocument doc = new ODocument(clazz);
        doc.field("test", "Some Value");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(doc);
        ser = baos.toByteArray();
      }
    }

    ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
    ODocument doc = (ODocument) input.readObject();

    assertEquals(doc.getClassName(), "Test");
    assertEquals(doc.field("test"), "Some Value");
  }

  @Test
  public void testDeserializationSave() throws IOException, ClassNotFoundException {
    ODocument doc = new ODocument("Test");
    doc.field("test", "Some Value");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);
    byte[] ser = baos.toByteArray();

    try (OxygenDB ctx = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + DocumentIndependentJavaSerializationTest.class.getSimpleName()
              + " memory users (admin identified by 'adminpwd' role admin)");
      try (var db =
          ctx.open(
              DocumentIndependentJavaSerializationTest.class.getSimpleName(),
              "admin",
              "adminpwd")) {

        OClass clazz = db.getMetadata().getSchema().createClass("Test");
        clazz.createProperty(db, "test", OType.STRING);
        ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
        ODocument doc1 = (ODocument) input.readObject();
        assertEquals(doc1.recordFormat, ((ODatabaseSessionInternal) db).getSerializer());
        assertEquals(doc1.getClassName(), "Test");
        assertEquals(doc1.field("test"), "Some Value");
      }
    }
  }
}
