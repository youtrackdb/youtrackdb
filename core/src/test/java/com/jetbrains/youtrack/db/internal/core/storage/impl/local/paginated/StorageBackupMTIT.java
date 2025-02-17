package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBEmbedded;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class StorageBackupMTIT {

  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile boolean stop = false;
  private YouTrackDB youTrackDB;
  private String dbName;

  @Test
  public void testParallelBackup() throws Exception {
    final var buildDirectory = System.getProperty("buildDirectory", ".");
    dbName = StorageBackupMTIT.class.getSimpleName();
    final var dbDirectory =
        buildDirectory + File.separator + "databases" + File.separator + dbName;
    final var backupDir = new File(buildDirectory, "backupDir");
    final var backupDbName = StorageBackupMTIT.class.getSimpleName() + "BackUp";

    FileUtils.deleteRecursively(new File(dbDirectory));

    try {

      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
          YouTrackDBConfig.defaultConfig());
      youTrackDB.execute(
          "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

      var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

      final Schema schema = db.getMetadata().getSchema();
      final var backupClass = schema.createClass("BackupClass");
      backupClass.createProperty(db, "num", PropertyType.INTEGER);
      backupClass.createProperty(db, "data", PropertyType.BINARY);

      backupClass.createIndex(db, "backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

      FileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) {
        Assert.assertTrue(backupDir.mkdirs());
      }

      final var executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (var i = 0; i < 4; i++) {
        futures.add(executor.submit(new DataWriterCallable()));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      latch.countDown();

      TimeUnit.MINUTES.sleep(15);

      stop = true;

      for (var future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.toPath());

      youTrackDB.close();

      final var backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
      FileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      var embedded =
          (YouTrackDBEmbedded)
              YouTrackDBInternal.embedded(buildDirectory, YouTrackDBConfig.defaultConfig());
      embedded.restore(
          backupDbName,
          null,
          null,
          null,
          backupDir.getAbsolutePath(),
          YouTrackDBConfig.defaultConfig());
      embedded.close();

      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
          YouTrackDBConfig.defaultConfig());
      final var compare =
          new DatabaseCompare(
              (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
              (DatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      System.out.println("compare");
      var areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {
      if (youTrackDB != null && youTrackDB.isOpen()) {
        try {
          youTrackDB.close();
        } catch (Exception ex) {
          LogManager.instance().error(this, "", ex);
        }
      }
      try {
        youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.defaultConfig());
        youTrackDB.drop(dbName);
        youTrackDB.drop(backupDbName);

        youTrackDB.close();

        FileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        LogManager.instance().error(this, "", ex);
      }
    }
  }

  @Test
  public void testParallelBackupEncryption() throws Exception {
    final var buildDirectory = System.getProperty("buildDirectory", ".");
    final var backupDbName = StorageBackupMTIT.class.getSimpleName() + "BackUp";
    final var backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
    final var backupDir = new File(buildDirectory, "backupDir");

    dbName = StorageBackupMTIT.class.getSimpleName();
    var dbDirectory = buildDirectory + File.separator + dbName;

    final var config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "T1JJRU5UREJfSVNfQ09PTA==")
            .build();

    try {

      FileUtils.deleteRecursively(new File(dbDirectory));

      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.execute(
          "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

      var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

      final Schema schema = db.getMetadata().getSchema();
      final var backupClass = schema.createClass("BackupClass");
      backupClass.createProperty(db, "num", PropertyType.INTEGER);
      backupClass.createProperty(db, "data", PropertyType.BINARY);

      backupClass.createIndex(db, "backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

      FileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) {
        Assert.assertTrue(backupDir.mkdirs());
      }

      final var executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (var i = 0; i < 4; i++) {
        futures.add(executor.submit(new DataWriterCallable()));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      latch.countDown();

      TimeUnit.MINUTES.sleep(5);

      stop = true;

      for (var future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.toPath());

      youTrackDB.close();

      FileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      var embedded =
          (YouTrackDBEmbedded) YouTrackDBInternal.embedded(buildDirectory, config);
      embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
      embedded.close();

      GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
          YouTrackDBConfig.defaultConfig());
      final var compare =
          new DatabaseCompare(
              (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
              (DatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);
      System.out.println("compare");

      var areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {
      if (youTrackDB != null && youTrackDB.isOpen()) {
        try {
          youTrackDB.close();
        } catch (Exception ex) {
          LogManager.instance().error(this, "", ex);
        }
      }
      try {
        youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
        youTrackDB.drop(dbName);
        youTrackDB.drop(backupDbName);

        youTrackDB.close();

        FileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        LogManager.instance().error(this, "", ex);
      }
    }
  }

  private final class DataWriterCallable implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      latch.await();

      System.out.println(Thread.currentThread() + " - start writing");

      try (var session = youTrackDB.open(dbName, "admin", "admin")) {
        final var random = new Random();
        while (!stop) {
          try {
            final var data = new byte[16];
            random.nextBytes(data);

            final var num = random.nextInt();

            session.begin();
            final var document = ((EntityImpl) session.newEntity("BackupClass"));
            document.field("num", num);
            document.field("data", data);
            session.commit();
          } catch (ModificationOperationProhibitedException e) {
            System.out.println("Modification prohibited ... wait ...");
            //noinspection BusyWait
            Thread.sleep(1000);
          } catch (Exception | Error e) {
            LogManager.instance().error(this, "", e);
            throw e;
          }
        }
      }

      System.out.println(Thread.currentThread() + " - done writing");

      return null;
    }
  }

  public final class DBBackupCallable implements Callable<Void> {

    private final String backupPath;

    public DBBackupCallable(String backupPath) {
      this.backupPath = backupPath;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      try (var db = youTrackDB.open(dbName, "admin", "admin")) {
        System.out.println(Thread.currentThread() + " - start backup");
        while (!stop) {
          TimeUnit.MINUTES.sleep(1);

          System.out.println(Thread.currentThread() + " do inc backup");
          db.incrementalBackup(Path.of(backupPath));
          System.out.println(Thread.currentThread() + " done inc backup");
        }
      } catch (Exception | Error e) {
        LogManager.instance().error(this, "", e);
        throw e;
      }

      System.out.println(Thread.currentThread() + " - stop backup");

      return null;
    }
  }
}
