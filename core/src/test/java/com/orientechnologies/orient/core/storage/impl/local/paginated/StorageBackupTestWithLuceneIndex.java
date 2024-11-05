package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class StorageBackupTestWithLuceneIndex {

  private String buildDirectory;

  private ODatabaseSessionInternal db;
  private String dbDirectory;
  private String backedUpDbDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    dbDirectory =
        buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));
    db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
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

    final ODatabaseSessionInternal backedUpDb =
        new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
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
    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex(
        "backupLuceneIndex",
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"name"});

    db.begin();
    final ODocument document = new ODocument("BackupClass");
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

    storage.close(true);

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseSessionInternal backedUpDb =
        new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true);

    var orientDB = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (ODatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (ODatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());
  }

  // @Test
  public void testSingeThreadIncrementalBackup() throws IOException {

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex(
        "backupLuceneIndex",
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"name"});

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.begin();
    ODocument document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    db.begin();
    document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage1");
    document.save();
    db.commit();

    db.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true);

    final String backedUpDbDirectory =
        buildDirectory
            + File.separator
            + StorageBackupTestWithLuceneIndex.class.getSimpleName()
            + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseSessionInternal backedUpDb =
        new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true);

    var orientDB = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (ODatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName(), "admin", "admin"),
            (ODatabaseSessionInternal)
                orientDB.open(
                    StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp",
                    "admin",
                    "admin"),
            System.out::println);
    Assert.assertTrue(compare.compare());
  }
}
