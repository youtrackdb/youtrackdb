package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.command.OCommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaImportExport extends DBTestBase {

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
        (YTDatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      YTClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty(db, "some", YTType.STRING);
      clazz.setCustom(db, "testcustom", "test");
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
        (YTDatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      YTClass clas1 = db1.getMetadata().getSchema().getClass("Test");
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
        (YTDatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      YTClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty(db, "bla", YTType.STRING).setDefaultValue(db, "something");
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
        (YTDatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();

      YTClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      YTProperty prop1 = clas1.getProperty("bla");
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
        (YTDatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      YTClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.addSuperClass(db, db.getMetadata().getSchema().getClass("ORestricted"));
      clazz.addSuperClass(db, db.getMetadata().getSchema().getClass("OIdentity"));

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
        (YTDatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              db1, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      YTClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertTrue(clas1.isSubClassOf("OIdentity"));
      Assert.assertTrue(clas1.isSubClassOf("ORestricted"));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  private static final class MockOutputListener implements OCommandOutputListener {

    @Override
    public void onMessage(String iText) {
    }
  }
}
