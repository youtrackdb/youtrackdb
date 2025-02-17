package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException;
import com.jetbrains.youtrack.db.api.record.RID;
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
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;

public class StorageBackupMTTest {

  private final CountDownLatch started = new CountDownLatch(1);
  private final Stack<CountDownLatch> backupIterationRecordCount = new Stack<>();
  private final CountDownLatch finished = new CountDownLatch(1);

  private YouTrackDB youTrackDB;
  private String dbName;

  @Test
  public void testParallelBackup() throws Exception {
    backupIterationRecordCount.clear();
    for (var i = 0; i < 100; i++) {
      var latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }
    var testDirectory = DbTestBase.getDirectoryPath(getClass());
    FileUtils.createDirectoryTree(testDirectory);
    dbName = StorageBackupMTTest.class.getSimpleName();
    final var dbDirectory = testDirectory + "databases" + File.separator + dbName;

    final var backupDir = new File(testDirectory, "backupDir");
    final var backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";

    FileUtils.deleteRecursively(new File(dbDirectory));

    try {

      youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
          YouTrackDBConfig.defaultConfig());
      youTrackDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

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
        var producerIterationRecordCount = new Stack<CountDownLatch>();
        producerIterationRecordCount.addAll(backupIterationRecordCount);
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (var future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.toPath());

      youTrackDB.close();

      final var backedUpDbDirectory = testDirectory + File.separator + backupDbName;
      FileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      var embedded =
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

      youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
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
        youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
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
    backupIterationRecordCount.clear();
    for (var i = 0; i < 100; i++) {
      var latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }

    var testDirectory = DbTestBase.getDirectoryPath(getClass());
    FileUtils.createDirectoryTree(testDirectory);

    final var backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";
    final var backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    final var backupDir = new File(testDirectory, "backupDir");

    dbName = StorageBackupMTTest.class.getSimpleName();
    var dbDirectory = testDirectory + File.separator + dbName;

    final var config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "T1JJRU5UREJfSVNfQ09PTA==")
            .build();

    try {

      FileUtils.deleteRecursively(new File(dbDirectory));

      youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, config);

      youTrackDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

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
        var producerIterationRecordCount = new Stack<CountDownLatch>();
        producerIterationRecordCount.addAll(backupIterationRecordCount);
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (var future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.toPath());

      youTrackDB.close();

      FileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      var embedded =
          (YouTrackDBEmbedded) YouTrackDBInternal.embedded(testDirectory, config);
      embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
      embedded.close();

      GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
      youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory,
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
      if (youTrackDB.isOpen()) {
        try {
          youTrackDB.close();
        } catch (Exception ex) {
          LogManager.instance().error(this, "", ex);
        }
      }
      try {
        youTrackDB = new YouTrackDBImpl("embedded:" + testDirectory, config);
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

    private final Stack<CountDownLatch> producerIterationRecordCount;
    private final int count;

    public DataWriterCallable(Stack<CountDownLatch> producerIterationRecordCount, int count) {
      this.producerIterationRecordCount = producerIterationRecordCount;
      this.count = count;
    }

    @Override
    public Void call() throws Exception {
      started.await();

      System.out.println(Thread.currentThread() + " - start writing");

      try (var db = youTrackDB.open(dbName, "admin", "admin")) {

        var random = new Random();
        List<RID> ids = new ArrayList<>();
        while (!producerIterationRecordCount.isEmpty()) {

          for (var i = 0; i < count; i++) {
            try {
              db.begin();
              final var data = new byte[random.nextInt(1024)];
              random.nextBytes(data);

              final var num = random.nextInt();
              if (!ids.isEmpty() && i % 8 == 0) {
                var id = ids.removeFirst();
                var record = db.load(id);
                db.delete(record);
              } else if (!ids.isEmpty() && i % 4 == 0) {
                var id = ids.removeFirst();
                final EntityImpl document = db.load(id);
                document.field("data", data);
              } else {
                final var document = ((EntityImpl) db.newEntity("BackupClass"));
                document.field("num", num);
                document.field("data", data);

                document.save();
                RID id = document.getIdentity();
                if (ids.size() < 100) {
                  ids.add(id);
                }
              }
              db.commit();

            } catch (ModificationOperationProhibitedException e) {
              System.out.println("Modification prohibited ... wait ...");
              //noinspection BusyWait
              Thread.sleep(1000);
            } catch (Exception | Error e) {
              e.printStackTrace();
              throw e;
            }
          }
          producerIterationRecordCount.pop().countDown();
          System.out.println(Thread.currentThread() + " writing of a batch done");
        }

        System.out.println(Thread.currentThread() + " - done writing");
        finished.countDown();
        return null;
      }
    }
  }

  public final class DBBackupCallable implements Callable<Void> {

    private final String backupPath;

    public DBBackupCallable(String backupPath) {
      this.backupPath = backupPath;
    }

    @Override
    public Void call() throws Exception {
      started.await();

      try (var db = youTrackDB.open(dbName, "admin", "admin")) {
        System.out.println(Thread.currentThread() + " - start backup");
        while (!backupIterationRecordCount.isEmpty()) {
          var latch = backupIterationRecordCount.pop();
          latch.await();

          System.out.println(Thread.currentThread() + " do inc backup");
          db.incrementalBackup(Path.of(backupPath));
          System.out.println(Thread.currentThread() + " done inc backup");
        }
      } catch (Exception | Error e) {
        LogManager.instance().error(this, "", e);
        throw e;
      }
      finished.countDown();

      System.out.println(Thread.currentThread() + " - stop backup");

      return null;
    }
  }
}
