package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.db.OxygenDBEmbedded;
import com.orientechnologies.orient.core.db.OxygenDBInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageBackupTest {

  private String buildDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
  }

  @Test
  public void testSingeThreadFullBackup() {
    final String dbName = StorageBackupTest.class.getSimpleName();
    final String dbDirectory = buildDirectory + File.separator + dbName;

    OFileUtils.deleteRecursively(new File(dbDirectory));

    OxygenDB oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());
    oxygenDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin");

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", OType.INTEGER);
    backupClass.createProperty(db, "data", OType.BINARY);

    backupClass.createIndex(db, "backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    oxygenDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    OxygenDBEmbedded embedded =
        (OxygenDBEmbedded)
            OxygenDBInternal.embedded(buildDirectory, OxygenDBConfig.defaultConfig());
    embedded.restore(
        backupDbName,
        null,
        null,
        null,
        backupDir.getAbsolutePath(),
        OxygenDBConfig.defaultConfig());
    embedded.close();

    oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin"),
            (ODatabaseSessionInternal) oxygenDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (oxygenDB.isOpen()) {
      oxygenDB.close();
    }

    oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());
    oxygenDB.drop(dbName);
    oxygenDB.drop(backupDbName);

    oxygenDB.close();

    OFileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackup() {
    final String dbDirectory =
        buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    OxygenDB oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());

    final String dbName = StorageBackupTest.class.getSimpleName();
    oxygenDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin");

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", OType.INTEGER);
    backupClass.createProperty(db, "data", OType.BINARY);

    backupClass.createIndex(db, "backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
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

        final ODocument document = new ODocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    oxygenDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    OxygenDBEmbedded embedded =
        (OxygenDBEmbedded)
            OxygenDBInternal.embedded(buildDirectory, OxygenDBConfig.defaultConfig());
    embedded.restore(
        backupDbName,
        null,
        null,
        null,
        backupDir.getAbsolutePath(),
        OxygenDBConfig.defaultConfig());
    embedded.close();

    oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());
    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin"),
            (ODatabaseSessionInternal) oxygenDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (oxygenDB.isOpen()) {
      oxygenDB.close();
    }

    oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());
    oxygenDB.drop(dbName);
    oxygenDB.drop(backupDbName);

    oxygenDB.close();

    OFileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackupEncryption() {
    final String dbDirectory =
        buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final OxygenDBConfig config =
        OxygenDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    OxygenDB oxygenDB = new OxygenDB("embedded:" + buildDirectory, config);

    final String dbName = StorageBackupTest.class.getSimpleName();
    oxygenDB.execute(
        "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

    var db = (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin");

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty(db, "num", OType.INTEGER);
    backupClass.createProperty(db, "data", OType.BINARY);

    backupClass.createIndex(db, "backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      db.begin();
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
      db.commit();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
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

        final ODocument document = new ODocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
        db.commit();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());
    db.close();

    oxygenDB.close();

    final String backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    OxygenDBEmbedded embedded =
        (OxygenDBEmbedded) OxygenDBInternal.embedded(buildDirectory, config);
    embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
    embedded.close();

    OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    oxygenDB = new OxygenDB("embedded:" + buildDirectory, OxygenDBConfig.defaultConfig());

    final ODatabaseCompare compare =
        new ODatabaseCompare(
            (ODatabaseSessionInternal) oxygenDB.open(dbName, "admin", "admin"),
            (ODatabaseSessionInternal) oxygenDB.open(backupDbName, "admin", "admin"),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (oxygenDB.isOpen()) {
      oxygenDB.close();
    }

    OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);

    oxygenDB = new OxygenDB("embedded:" + buildDirectory, config);
    oxygenDB.drop(dbName);
    oxygenDB.drop(backupDbName);

    oxygenDB.close();

    OFileUtils.deleteRecursively(backupDir);

    OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);
  }
}
