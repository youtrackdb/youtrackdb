package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tglman on 23/05/16.
 */
public class ODatabaseImportTest {

  @Test
  public void exportImportOnlySchemaTest() throws IOException {
    String databaseName = "export";
    final String exportDbPath = "target/export_" + ODatabaseImportTest.class.getSimpleName();
    OrientDB orientDB = OrientDB.embedded(exportDbPath, OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final ODatabaseSession db = orientDB.open(databaseName, "admin", "admin")) {
      db.createClass("SimpleClass");

      final ODatabaseExport export =
          new ODatabaseExport((ODatabaseSessionInternal) db, output, iText -> {});
      export.setOptions(" -excludeAll -includeSchema=true");
      export.exportDatabase();
    }
    orientDB.drop(databaseName);
    orientDB.close();

    final String importDbPath = "target/import_" + ODatabaseImportTest.class.getSimpleName();
    orientDB = OrientDB.embedded(importDbPath, OrientDBConfig.defaultConfig());
    databaseName = "import";

    orientDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");
    try (final ODatabaseSession db = orientDB.open(databaseName, "admin", "admin")) {
      final ODatabaseImport importer =
          new ODatabaseImport(
              (ODatabaseSessionInternal) db,
              new ByteArrayInputStream(output.toByteArray()),
              iText -> {});
      importer.importDatabase();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("SimpleClass"));
    }
    orientDB.drop(databaseName);
    orientDB.close();
  }
}
