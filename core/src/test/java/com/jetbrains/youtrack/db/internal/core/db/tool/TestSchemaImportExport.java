package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaImportExport extends DbTestBase {

  @Test
  public void testExportImportCustomData() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    var output = new ByteArrayOutputStream();
    try (var db =
        (DatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      var clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty(db, "some", PropertyType.STRING);
      clazz.setCustom(db, "testcustom", "test");
      var exp = new DatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var sessionOne =
        (DatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      var imp =
          new DatabaseImport(
              sessionOne, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      var clas1 = sessionOne.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      Assert.assertEquals("test", clas1.getCustom(sessionOne, "testcustom"));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  @Test
  public void testExportImportDefaultValue() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    var output = new ByteArrayOutputStream();

    try (var db =
        (DatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      var clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty(db, "bla", PropertyType.STRING).setDefaultValue(db, "something");
      var exp = new DatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var sessionOne =
        (DatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      var imp =
          new DatabaseImport(
              sessionOne, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();

      var clas1 = sessionOne.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      var prop1 = clas1.getProperty(sessionOne, "bla");
      Assert.assertNotNull(prop1);
      Assert.assertEquals("something", prop1.getDefaultValue(sessionOne));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  @Test
  public void testExportImportMultipleInheritance() throws IOException {
    context.createIfNotExists(
        TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    var output = new ByteArrayOutputStream();
    try (var db =
        (DatabaseSessionInternal)
            context.open(TestSchemaImportExport.class.getSimpleName(), "admin", "admin")) {
      var clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.addSuperClass(db, db.getMetadata().getSchema().getClass("ORestricted"));
      clazz.addSuperClass(db, db.getMetadata().getSchema().getClass("OIdentity"));

      var exp = new DatabaseExport(db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      context.drop(TestSchemaImportExport.class.getSimpleName());
    }

    context.createIfNotExists(
        "imp_" + TestSchemaImportExport.class.getSimpleName(),
        DatabaseType.MEMORY,
        "admin",
        "admin",
        "admin");
    try (var sessionOne =
        (DatabaseSessionInternal)
            context.open("imp_" + TestSchemaImportExport.class.getSimpleName(),
                "admin", "admin")) {
      var imp =
          new DatabaseImport(
              sessionOne, new ByteArrayInputStream(output.toByteArray()), new MockOutputListener());
      imp.importDatabase();
      var clas1 = sessionOne.getMetadata().getSchema().getClass("Test");
      Assert.assertTrue(clas1.isSubClassOf(sessionOne, "OIdentity"));
      Assert.assertTrue(clas1.isSubClassOf(sessionOne, "ORestricted"));
    } finally {
      context.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
  }

  private static final class MockOutputListener implements CommandOutputListener {

    @Override
    public void onMessage(String iText) {
    }
  }
}
