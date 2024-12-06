/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.AutomaticBackup;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class LuceneAutomaticBackupRestoreTest {

  private static final String DBNAME = "OLuceneAutomaticBackupRestoreTest";
  private File tempFolder;

  @Rule
  public TestName name = new TestName();

  private YouTrackDB youTrackDB;
  private String URL = null;
  private String BACKUPDIR = null;
  private String BACKUFILE = null;

  private OServer server;
  private DatabaseSessionInternal db;

  @Before
  public void setUp() throws Exception {

    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    Assume.assumeFalse(os.contains("win"));

    final String buildDirectory = System.getProperty("buildDirectory", "target");
    final File buildDirectoryFile = new File(buildDirectory);

    tempFolder = new File(buildDirectoryFile, name.getMethodName());
    FileUtils.deleteRecursively(tempFolder);
    Assert.assertTrue(tempFolder.mkdirs());

    System.setProperty("YOU_TRACK_DB_HOME", tempFolder.getCanonicalPath());

    String path = tempFolder.getCanonicalPath() + File.separator + "databases";
    server =
        new OServer(false) {
          @Override
          public Map<String, String> getAvailableStorageNames() {
            HashMap<String, String> result = new HashMap<>();
            result.put(DBNAME, URL);
            return result;
          }
        };
    server.startup();

    youTrackDB = server.getContext();

    URL = "plocal:" + path + File.separator + DBNAME;

    BACKUPDIR = tempFolder.getCanonicalPath() + File.separator + "backups";

    BACKUFILE = BACKUPDIR + File.separator + DBNAME;

    File config = new File(tempFolder, "config");
    Assert.assertTrue(config.mkdirs());

    dropIfExists();

    youTrackDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin) ", DBNAME);

    db = (DatabaseSessionInternal) youTrackDB.open(DBNAME, "admin", "admin");

    db.command("create class City ");
    db.command("create property City.name string");
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");

    EntityImpl doc = new EntityImpl("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
  }

  private void dropIfExists() {

    if (youTrackDB.exists(DBNAME)) {
      youTrackDB.drop(DBNAME);
    }
  }

  @After
  public void tearDown() throws Exception {
    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    if (!os.contains("win")) {
      dropIfExists();

      FileUtils.deleteRecursively(tempFolder);
    }
  }

  @AfterClass
  public static void afterClass() {
    final YouTrackDBManager youTrack = YouTrackDBManager.instance();

    if (youTrack != null) {
      youTrack.shutdown();
      youTrack.startup();
    }
  }

  @Test
  public void shouldExportImport() throws IOException, InterruptedException {

    try (ResultSet query = db.query("select from City where name lucene 'Rome'")) {
      assertThat(query).hasSize(1);
    }

    String jsonConfig =
        IOUtils.readStreamAsString(
            getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    EntityImpl doc = new EntityImpl();
    doc.fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.json");

    doc.field("targetDirectory", BACKUPDIR);
    doc.field("mode", "EXPORT");

    doc.field("dbInclude", new String[]{"OLuceneAutomaticBackupRestoreTest"});

    doc.field(
        "firstTime",
        new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    IOUtils.writeFile(new File(tempFolder, "config/automatic-backup.json"), doc.toJSON());

    final AutomaticBackup aBackup = new AutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[]{};

    aBackup.config(server, config);
    final CountDownLatch latch = new CountDownLatch(1);

    aBackup.registerListener(
        new AutomaticBackup.OAutomaticBackupListener() {
          @Override
          public void onBackupCompleted(String database) {
            latch.countDown();
          }

          @Override
          public void onBackupError(String database, Exception e) {
            latch.countDown();
          }
        });
    latch.await();
    aBackup.sendShutdown();

    db.close();

    dropIfExists();
    // RESTORE

    db = createAndOpen();

    try (final GZIPInputStream stream =
        new GZIPInputStream(new FileInputStream(BACKUFILE + ".json.gz"))) {
      new DatabaseImport(db, stream, s -> {
      }).importDatabase();
    }

    db.close();

    // VERIFY
    db = open();

    assertThat(db.countClass("City")).isEqualTo(1);

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(SchemaClass.INDEX_TYPE.FULLTEXT.name());

    assertThat(db.query("select from City where name lucene 'Rome'")).hasSize(1);
  }

  private DatabaseSessionInternal createAndOpen() {
    youTrackDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin) ", DBNAME);
    return open();
  }

  private DatabaseSessionInternal open() {
    return (DatabaseSessionInternal) youTrackDB.open(DBNAME, "admin", "admin");
  }
}
