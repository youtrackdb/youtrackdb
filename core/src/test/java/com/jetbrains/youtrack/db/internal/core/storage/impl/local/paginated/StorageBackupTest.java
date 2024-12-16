package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBEmbedded;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.File;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageBackupTest {

  private String testDirectory;

  @Before
  public void before() {
    testDirectory = DbTestBase.getDirectoryPath(getClass());
  }

  @Test
  public void testSingeThreadFullBackup() {
    final String dbName = StorageBackupTest.class.getSimpleName();
    final String dbDirectory = testDirectory + File.separator + dbName;

    FileUtils.deleteRecursively(new File(dbDirectory));

    YouTrackDB youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", PropertyType.INTEGER);
    backupClass.createProperty(db, "data", PropertyType.BINARY);

    backupClass.createIndex(db, "backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final EntityImpl document = new EntityImpl("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.toPath());
    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;

    FileUtils.deleteRecursively(new File(backedUpDbDirectory));

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

    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    final DatabaseCompare compare =
        new DatabaseCompare(
            (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (DatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    FileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackup() {
    final String dbDirectory =
        testDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    FileUtils.deleteRecursively(new File(dbDirectory));

    YouTrackDB youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
        YouTrackDBConfig.defaultConfig());

    final String dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", PropertyType.INTEGER);
    backupClass.createProperty(db, "data", PropertyType.BINARY);

    backupClass.createIndex(db, "backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final EntityImpl document = new EntityImpl("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.toPath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        db.begin();
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final EntityImpl document = new EntityImpl("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.toPath());
    }

    db.incrementalBackup(backupDir.toPath());
    db.close();

    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    FileUtils.deleteRecursively(new File(backedUpDbDirectory));

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

    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    final DatabaseCompare compare =
        new DatabaseCompare(
            (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (DatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    FileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackupEncryption() {
    final String dbDirectory =
        testDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    FileUtils.deleteRecursively(new File(dbDirectory));

    final YouTrackDBConfigImpl config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    YouTrackDB youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, config);

    final String dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", PropertyType.INTEGER);
    backupClass.createProperty(db, "data", PropertyType.BINARY);

    backupClass.createIndex(db, "backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final EntityImpl document = new EntityImpl("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.toPath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        db.begin();
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final EntityImpl document = new EntityImpl("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.toPath());
    }

    db.incrementalBackup(backupDir.toPath());
    db.close();

    youTrackDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    FileUtils.deleteRecursively(new File(backedUpDbDirectory));

    YouTrackDBEmbedded embedded =
        (YouTrackDBEmbedded) YouTrackDBInternal.embedded(testDirectory, config);
    embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
    embedded.close();

    GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());

    final DatabaseCompare compare =
        new DatabaseCompare(
            (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
            (DatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);

    youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, config);
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    FileUtils.deleteRecursively(backupDir);

    GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);
  }
}
