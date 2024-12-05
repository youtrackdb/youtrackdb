/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DbImportExportTest extends DocumentDBBaseTest implements OCommandOutputListener {

  public static final String EXPORT_FILE_PATH = "target/export/db.export.gz";
  public static final String IMPORT_DB_NAME = "test-import";
  public static final String IMPORT_DB_PATH = "target/import";

  private final String testPath;
  private final String exportFilePath;
  private boolean dumpMode = false;

  @Parameters(value = {"remote", "testPath"})
  public DbImportExportTest(boolean remote, String testPath) {
    super(remote);
    this.testPath = testPath;
    this.exportFilePath = System.getProperty("exportFilePath", EXPORT_FILE_PATH);
  }

  @Test
  public void testDbExport() throws IOException {
    // ADD A CUSTOM TO THE CLASS
    database.command("alter class V custom onBeforeCreate=onBeforeCreateItem").close();

    final ODatabaseExport export =
        new ODatabaseExport(database, testPath + "/" + exportFilePath, this);
    export.exportDatabase();
    export.close();
  }

  @Test(dependsOnMethods = "testDbExport")
  public void testDbImport() throws IOException {
    final File importDir = new File(testPath + "/" + IMPORT_DB_PATH);
    if (importDir.exists()) {
      for (final File f : importDir.listFiles()) {
        f.delete();
      }
    } else {
      importDir.mkdir();
    }

    try (YouTrackDB youTrackDBImport =
        YouTrackDB.embedded(
            testPath + File.separator + IMPORT_DB_PATH, YouTrackDBConfig.defaultConfig())) {
      youTrackDBImport.createIfNotExists(
          IMPORT_DB_NAME, ODatabaseType.PLOCAL, "admin", "admin", "admin");
      try (var importDB = youTrackDBImport.open(IMPORT_DB_NAME, "admin", "admin")) {
        final ODatabaseImport dbImport =
            new ODatabaseImport(
                (YTDatabaseSessionInternal) importDB, testPath + "/" + exportFilePath, this);
        // UNREGISTER ALL THE HOOKS
        for (final YTRecordHook hook : new ArrayList<>(database.getHooks().keySet())) {
          database.unregisterHook(hook);
        }
        dbImport.setDeleteRIDMapping(false);
        dbImport.importDatabase();
        dbImport.close();
      }
    }
  }

  @Test(dependsOnMethods = "testDbImport")
  public void testCompareDatabases() throws IOException {
    if (remoteDB) {
      final String env = getTestEnv();
      if (env == null || env.equals("dev")) {
        return;
      }
      // EXECUTES ONLY IF NOT REMOTE ON CI/RELEASE TEST ENV
    }
    try (YouTrackDB youTrackDBImport =
        YouTrackDB.embedded(
            testPath + File.separator + IMPORT_DB_PATH, YouTrackDBConfig.defaultConfig())) {
      try (var importDB = youTrackDBImport.open(IMPORT_DB_NAME, "admin", "admin")) {
        final ODatabaseCompare databaseCompare =
            new ODatabaseCompare(database, (YTDatabaseSessionInternal) importDB, this);
        databaseCompare.setCompareEntriesForAutomaticIndexes(true);
        databaseCompare.setCompareIndexMetadata(true);
        Assert.assertTrue(databaseCompare.compare());
      }
    }
  }

  @Test
  public void embeddedListMigration() throws Exception {
    if (remoteDB) {
      return;
    }

    final File localTesPath = new File(testPath + "/target", "embeddedListMigration");
    OFileUtils.deleteRecursively(localTesPath);
    Assert.assertTrue(localTesPath.mkdirs());

    final File exportPath = new File(localTesPath, "export.json.gz");

    final YouTrackDBConfig config =
        new YouTrackDBConfigBuilder()
            .addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, true)
            .build();
    try (final YouTrackDB youTrackDB = new YouTrackDB("embedded:" + localTesPath.getPath(),
        config)) {
      youTrackDB.create("original", ODatabaseType.PLOCAL);

      try (final YTDatabaseSessionInternal session = (YTDatabaseSessionInternal) youTrackDB.open(
          "original", "admin", "admin")) {
        final YTSchema schema = session.getMetadata().getSchema();

        final YTClass rootCls = schema.createClass("RootClass");
        rootCls.createProperty(session, "embeddedList", YTType.EMBEDDEDLIST);

        final YTClass childCls = schema.createClass("ChildClass");

        final List<YTRID> ridsToDelete = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          session.begin();
          final YTEntityImpl document = new YTEntityImpl(childCls);
          document.save();
          session.commit();

          ridsToDelete.add(document.getIdentity());
        }

        for (final YTRID rid : ridsToDelete) {
          session.begin();
          rid.getRecord().delete();
          session.commit();
        }

        final YTEntityImpl rootDocument = new YTEntityImpl(rootCls);
        final ArrayList<YTEntityImpl> documents = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
          session.begin();
          final YTEntityImpl embeddedDocument = new YTEntityImpl();

          final YTEntityImpl doc = new YTEntityImpl(childCls);
          doc.save();
          session.commit();

          embeddedDocument.field("link", doc.getIdentity());
          documents.add(embeddedDocument);
        }

        session.begin();
        rootDocument.field("embeddedList", documents);
        rootDocument.save();
        session.commit();

        final ODatabaseExport databaseExport =
            new ODatabaseExport(
                session, exportPath.getPath(), System.out::println);
        databaseExport.exportDatabase();
      }

      youTrackDB.create("imported", ODatabaseType.PLOCAL);
      try (final YTDatabaseSessionInternal session =
          (YTDatabaseSessionInternal) youTrackDB.open("imported", "admin", "admin")) {
        final ODatabaseImport databaseImport =
            new ODatabaseImport(session, exportPath.getPath(), System.out::println);
        databaseImport.run();

        final Iterator<YTEntityImpl> classIterator = session.browseClass("RootClass");
        final YTEntityImpl rootDocument = classIterator.next();

        final List<YTEntityImpl> documents = rootDocument.field("embeddedList");
        for (int i = 0; i < 10; i++) {
          final YTEntityImpl embeddedDocument = documents.get(i);

          embeddedDocument.setLazyLoad(false);
          final YTRecordId link = embeddedDocument.getProperty("link");

          Assert.assertNotNull(link);
          Assert.assertNotNull(link.getRecord());
        }
      }
    }
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    if (iText != null && iText.contains("ERR")) {
      // ACTIVATE DUMP MODE
      dumpMode = true;
    }
    if (dumpMode) {
      OLogManager.instance().error(this, iText, null);
    }
  }
}
