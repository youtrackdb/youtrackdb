package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBEmbedded;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.io.File;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageBackupTest {

  private String testDirectory;

  @Before
  public void before() {
    testDirectory = DBTestBase.getDirectoryPath(getClass());
  }

  @Test
  public void testSingeThreadFullBackup() {
    final String dbName = StorageBackupTest.class.getSimpleName();
    final String dbDirectory = testDirectory + File.separator + dbName;

    OFileUtils.deleteRecursively(new File(dbDirectory));

    YouTrackDB youTrackDB = new YouTrackDB("embedded:" + testDirectory,
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", YTType.INTEGER);
    backupClass.createProperty(db, "data", YTType.BINARY);

    backupClass.createIndex(db, "backupIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final YTDocument document = new YTDocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    YouTrackDBEmbedded embedded =
        (YouTrackDBEmbedded)
            YouTrackDBInternal.embedded(testDirectory, YouTrackDBConfig.defaultConfig());
    embedded.restore(
        backupDbName,
        null,
        null,
        null,
        backupDir.getAbsolutePath(),
        YouTrackDBConfig.defaultConfig());
    embedded.close();

    youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (YTDatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    OFileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackup() {
    final String dbDirectory =
        testDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    YouTrackDB youTrackDB = new YouTrackDB("embedded:" + testDirectory,
        YouTrackDBConfig.defaultConfig());

    final String dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", YTType.INTEGER);
    backupClass.createProperty(db, "data", YTType.BINARY);

    backupClass.createIndex(db, "backupIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final YTDocument document = new YTDocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        db.begin();
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final YTDocument document = new YTDocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    YouTrackDBEmbedded embedded =
        (YouTrackDBEmbedded)
            YouTrackDBInternal.embedded(testDirectory, YouTrackDBConfig.defaultConfig());
    embedded.restore(
        backupDbName,
        null,
        null,
        null,
        backupDir.getAbsolutePath(),
        YouTrackDBConfig.defaultConfig());
    embedded.close();

    youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (YTDatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    OFileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackupEncryption() {
    final String dbDirectory =
        testDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    YouTrackDB youTrackDB = new YouTrackDB("embedded:" + testDirectory, config);

    final String dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", YTType.INTEGER);
    backupClass.createProperty(db, "data", YTType.BINARY);

    backupClass.createIndex(db, "backupIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final YTDocument document = new YTDocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        db.begin();
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final YTDocument document = new YTDocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    YouTrackDBEmbedded embedded =
        (YouTrackDBEmbedded) YouTrackDBInternal.embedded(testDirectory, config);
    embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
    embedded.close();

    YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());

    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (YTDatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);

    youTrackDB = new YouTrackDB("embedded:" + testDirectory, config);
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    OFileUtils.deleteRecursively(backupDir);

    YTGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);
  }
}
