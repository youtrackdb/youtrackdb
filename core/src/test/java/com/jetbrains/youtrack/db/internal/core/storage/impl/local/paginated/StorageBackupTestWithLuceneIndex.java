package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class StorageBackupTestWithLuceneIndex {

  private String buildDirectory;

  private DatabaseSessionInternal db;
  private String dbDirectory;
  private String backedUpDbDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    dbDirectory =
        buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName();
    FileUtils.deleteRecursively(new File(dbDirectory));
    db = new DatabaseDocumentTx("plocal:" + dbDirectory);
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

    final DatabaseSessionInternal backedUpDb =
        new DatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    if (backedUpDb.exists()) {
      if (backedUpDb.isClosed()) {
        backedUpDb.open("admin", "admin");
        backedUpDb.drop();
      }
    }

    FileUtils.deleteRecursively(new File(dbDirectory));
    FileUtils.deleteRecursively(new File(buildDirectory, "backupDir"));
  }

  // @Test
  public void testSingeThreadFullBackup() throws IOException {
    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", PropertyType.INTEGER);
    backupClass.createProperty(db, "name", PropertyType.STRING);

    backupClass.createIndex(db,
        "backupLuceneIndex",
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"name"});

    db.begin();
    final EntityImpl document = new EntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();
    db.commit();

    final File backupDir = new File(buildDirectory, "backupDir");
    FileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    final Storage storage = db.getStorage();
    db.close();

    storage.close(db, true);

    FileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final DatabaseSessionInternal backedUpDb =
        new DatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final Storage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(db, true);

    var youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    final DatabaseCompare compare =
        new DatabaseCompare(
            (DatabaseSessionInternal)
                youTrackDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (DatabaseSessionInternal)
                youTrackDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());
  }

  // @Test
  public void testSingeThreadIncrementalBackup() throws IOException {

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", PropertyType.INTEGER);
    backupClass.createProperty(db, "name", PropertyType.STRING);

    backupClass.createIndex(db,
        "backupLuceneIndex",
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"name"});

    final File backupDir = new File(buildDirectory, "backupDir");
    FileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.begin();
    EntityImpl document = new EntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    db.begin();
    document = new EntityImpl("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage1");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    final Storage storage = db.getStorage();
    db.close();

    storage.close(db, true);

    final String backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
    FileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final DatabaseSessionInternal backedUpDb =
        new DatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final Storage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(db, true);

    var youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    final DatabaseCompare compare =
        new DatabaseCompare(
            (DatabaseSessionInternal)
                youTrackDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (DatabaseSessionInternal)
                youTrackDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);
    Assert.assertTrue(compare.compare());
  }
}
