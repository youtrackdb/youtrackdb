package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
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
    YouTrackDB youTrackDB = YouTrackDB.embedded(exportDbPath, YouTrackDBConfig.defaultConfig());
    youTrackDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final YTDatabaseSession db = youTrackDB.open(databaseName, "admin", "admin")) {
      db.createClass("SimpleClass");

      final ODatabaseExport export =
          new ODatabaseExport((YTDatabaseSessionInternal) db, output, iText -> {
          });
      export.setOptions(" -excludeAll -includeSchema=true");
      export.exportDatabase();
    }
    youTrackDB.drop(databaseName);
    youTrackDB.close();

    final String importDbPath = "target/import_" + ODatabaseImportTest.class.getSimpleName();
    youTrackDB = YouTrackDB.embedded(importDbPath, YouTrackDBConfig.defaultConfig());
    databaseName = "import";

    youTrackDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL, "admin", "admin", "admin");
    try (var db = (YTDatabaseSessionInternal) youTrackDB.open(databaseName, "admin",
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
    youTrackDB.drop(databaseName);
    youTrackDB.close();
  }
}
