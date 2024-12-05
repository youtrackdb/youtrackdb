package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class StorageBackupTestWithLuceneIndex {

  private String buildDirectory;

  private YTDatabaseSessionInternal db;
  private String dbDirectory;
  private String backedUpDbDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    dbDirectory =
        buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));
    db = new YTDatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
  }

  @After
  public void after() {
    if (db.exists()) {
      if (db.isClosed()) {
        db.open("admin", "admin");
      }
      db.drop();
    }

    final YTDatabaseSessionInternal backedUpDb =
        new YTDatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    if (backedUpDb.exists()) {
      if (backedUpDb.isClosed()) {
        backedUpDb.open("admin", "admin");
        backedUpDb.drop();
      }
    }

    OFileUtils.deleteRecursively(new File(dbDirectory));
    OFileUtils.deleteRecursively(new File(buildDirectory, "backupDir"));
  }

  // @Test
  public void testSingeThreadFullBackup() throws IOException {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", YTType.INTEGER);
    backupClass.createProperty(db, "name", YTType.STRING);

    backupClass.createIndex(db,
        "backupLuceneIndex",
        YTClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"name"});

    db.begin();
    final YTEntityImpl document = new YTEntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();
    db.commit();

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    final OStorage storage = db.getStorage();
    db.close();

    storage.close(db, true);

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final YTDatabaseSessionInternal backedUpDb =
        new YTDatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(db, true);

    var orientDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (YTDatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (YTDatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());
  }

  // @Test
  public void testSingeThreadIncrementalBackup() throws IOException {

    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", YTType.INTEGER);
    backupClass.createProperty(db, "name", YTType.STRING);

    backupClass.createIndex(db,
        "backupLuceneIndex",
        YTClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"name"});

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.begin();
    YTEntityImpl document = new YTEntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    db.begin();
    document = new YTEntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage1");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = db.getStorage();
    db.close();

    storage.close(db, true);

    final String backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final YTDatabaseSessionInternal backedUpDb =
        new YTDatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(db, true);

    var orientDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (YTDatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (YTDatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);
    Assert.assertTrue(compare.compare());
  }
}
