/*
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

package com.jetbrains.youtrack.db.internal.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.handler.AutomaticBackup;
import com.jetbrains.youtrack.db.internal.tools.config.ServerParameterConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 */
public class LuceneSpatialAutomaticBackupRestoreTest {

  private static final String DBNAME = "OLuceneAutomaticBackupRestoreTest";

  public File tempFolder;
  private YouTrackDB youTrackDB;
  private String URL = null;
  private String BACKUPDIR = null;
  private String BACKUFILE = null;

  private YouTrackDBServer server;
  private DatabaseSessionInternal db;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    Assume.assumeFalse(IOUtils.isOsWindows());

    final var buildDirectory = System.getProperty("buildDirectory", "target");
    final var buildDirectoryFile = new File(buildDirectory);

    tempFolder = new File(buildDirectoryFile, name.getMethodName());
    FileUtils.deleteRecursively(tempFolder);
    Assert.assertTrue(tempFolder.mkdirs());

    server =
        new YouTrackDBServer() {
          @Override
          public Map<String, String> getAvailableStorageNames() {
            var result = new HashMap<String, String>();
            result.put(DBNAME, URL);
            return result;
          }
        };
    server.startup();

    System.setProperty("YOUTRACKDB_HOME", tempFolder.getAbsolutePath());

    var path = tempFolder.getAbsolutePath() + File.separator + "databases";
    youTrackDB = server.getContext();

    URL = "plocal:" + path + File.separator + DBNAME;

    BACKUPDIR = tempFolder.getAbsolutePath() + File.separator + "backups";

    BACKUFILE = BACKUPDIR + File.separator + DBNAME;

    final var config = new File(tempFolder, "config");
    Assert.assertTrue(config.mkdirs());

    dropIfExists();

    youTrackDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", DBNAME);

    db = (DatabaseSessionInternal) youTrackDB.open(DBNAME, "admin", "admin");

    db.command("create class City ").close();
    db.command("create property City.name string").close();
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();

    db.command("create property City.location EMBEDDED OPOINT").close();

    db.command("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE").close();

    var rome = newCity("Rome", 12.5, 41.9);

    db.begin();
    db.commit();
  }

  protected EntityImpl newCity(String name, final Double longitude, final Double latitude) {
    var city =
        ((EntityImpl) db.newEntity("City"))
            .field("name", name)
            .field(
                "location",
                ((EntityImpl) db.newEntity("OPoint"))
                    .field(
                        "coordinates",
                        new ArrayList<Double>() {
                          {
                            add(longitude);
                            add(latitude);
                          }
                        }));
    return city;
  }

  private void dropIfExists() {

    if (youTrackDB.exists(DBNAME)) {
      youTrackDB.drop(DBNAME);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (!IOUtils.isOsWindows()) {
      dropIfExists();

      tempFolder.delete();
    }
  }

  @Test
  public void shouldExportImport() throws IOException, InterruptedException {
    var query =
        "select * from City where  ST_WITHIN(location,'POLYGON ((12.314015 41.8262816, 12.314015"
            + " 41.963125, 12.6605063 41.963125, 12.6605063 41.8262816, 12.314015 41.8262816))') ="
            + " true";
    var docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), 1);

    var jsonConfig =
        IOUtils.readStreamAsString(
            getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    var doc =
        ((EntityImpl) db.newEntity());
    doc.updateFromJSON(jsonConfig);
    doc.field("enabled", true)
        .field("targetFileName", "${DBNAME}.json")
        .field("targetDirectory", BACKUPDIR)
        .field("mode", "EXPORT")
        .field("dbInclude", new String[]{DBNAME})
        .field(
            "firstTime",
            new SimpleDateFormat("HH:mm:ss")
                .format(new Date(System.currentTimeMillis() + 2000)));

    IOUtils.writeFile(
        new File(tempFolder.getAbsolutePath() + "/config/automatic-backup.json"), doc.toJSON());

    final var aBackup = new AutomaticBackup();

    final var config = new ServerParameterConfiguration[]{};

    aBackup.config(server, config);
    final var latch = new CountDownLatch(1);

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

    var stream = new GZIPInputStream(new FileInputStream(BACKUFILE + ".json.gz"));
    new DatabaseImport(
        db,
        stream,
        new CommandOutputListener() {
          @Override
          public void onMessage(String s) {
          }
        })
        .importDatabase();

    db.close();

    // VERIFY
    db = open();

    assertThat(db.countClass("City")).isEqualTo(1);

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(SchemaClass.INDEX_TYPE.SPATIAL.name());

    assertThat(db.query(query).stream()).hasSize(1);
  }

  private DatabaseSessionInternal createAndOpen() {
    youTrackDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", DBNAME);
    return open();
  }

  private DatabaseSessionInternal open() {
    return (DatabaseSessionInternal) youTrackDB.open(DBNAME, "admin", "admin");
  }
}
