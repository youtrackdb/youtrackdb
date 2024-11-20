package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaImportExport extends BaseMemoryDatabase {
  @Test
  public void testExportImportCustomData() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (var db =
        (ODatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("some", OType.STRING);
      clazz.setCustom("testcustom", "test");
      ODatabaseExport exp = new ODatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var db1 =
        (ODatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      Assert.assertEquals("test", clas1.getCustom("testcustom"));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  @Test
  public void testExportImportDefaultValue() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    try (var db =
        (ODatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("bla", OType.STRING).setDefaultValue("something");
      ODatabaseExport exp = new ODatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var db1 =
        (ODatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();

      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      OProperty prop1 = clas1.getProperty("bla");
      Assert.assertNotNull(prop1);
      Assert.assertEquals("something", prop1.getDefaultValue());
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  @Test
  public void testExportImportMultipleInheritance() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (var db =
        (ODatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.addSuperClass(db.getMetadata().getSchema().getClass("ORestricted"));
      clazz.addSuperClass(db.getMetadata().getSchema().getClass("OIdentity"));

      ODatabaseExport exp = new ODatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        ODatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var db1 =
        (ODatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertTrue(clas1.isSubClassOf("OIdentity"));
      Assert.assertTrue(clas1.isSubClassOf("ORestricted"));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  private static final class MockOutputListener implements OCommandOutputListener {

    @Override
    public void onMessage(String iText) {}
  }
}
