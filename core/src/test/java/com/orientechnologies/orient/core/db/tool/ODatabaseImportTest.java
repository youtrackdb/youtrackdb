package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODatabaseImportTest {

  @Test
  public void exportImportOnlySchemaTest() throws IOException {
    String databaseName = "export";
    final String exportDbPath = "target/export_" + ODatabaseImportTest.class.getSimpleName();
    OxygenDB oxygenDB = OxygenDB.embedded(exportDbPath, OxygenDBConfig.defaultConfig());
    oxygenDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final ODatabaseSession db = oxygenDB.open(databaseName, "admin", "admin")) {
      db.createClass("SimpleClass");

      final ODatabaseExport export =
          new ODatabaseExport((ODatabaseSessionInternal) db, output, iText -> {
          });
      export.setOptions(" -excludeAll -includeSchema=true");
      export.exportDatabase();
    }
    oxygenDB.drop(databaseName);
    oxygenDB.close();

    final String importDbPath = "target/import_" + ODatabaseImportTest.class.getSimpleName();
    oxygenDB = OxygenDB.embedded(importDbPath, OxygenDBConfig.defaultConfig());
    databaseName = "import";

    oxygenDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");
    try (var db = (ODatabaseSessionInternal) oxygenDB.open(databaseName, "admin",
        "admin")) {
      final ODatabaseImport importer =
          new ODatabaseImport(
              db,
              new ByteArrayInputStream(output.toByteArray()),
              iText -> {
              });
      importer.importDatabase();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("SimpleClass"));
    }
    oxygenDB.drop(databaseName);
    oxygenDB.close();
  }
}
