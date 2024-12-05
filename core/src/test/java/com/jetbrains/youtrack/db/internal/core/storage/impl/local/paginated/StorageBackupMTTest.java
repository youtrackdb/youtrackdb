package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.concur.lock.YTModificationOperationProhibitedException;
import com.jetbrains.youtrack.db.internal.common.io.OFileUtils;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBEmbedded;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseCompare;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
    for (int i = 0; i < 100; i++) {
      CountDownLatch latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }
    String testDirectory = DBTestBase.getDirectoryPath(getClass());
    OFileUtils.createDirectoryTree(testDirectory);
    dbName = StorageBackupMTTest.class.getSimpleName();
    final String dbDirectory = testDirectory + "databases" + File.separator + dbName;

    final File backupDir = new File(testDirectory, "backupDir");
    final String backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";

    OFileUtils.deleteRecursively(new File(dbDirectory));

    try {

      youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
      youTrackDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

      var db = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

      final YTSchema schema = db.getMetadata().getSchema();
      final YTClass backupClass = schema.createClass("BackupClass");
      backupClass.createProperty(db, "num", YTType.INTEGER);
      backupClass.createProperty(db, "data", YTType.BINARY);

      backupClass.createIndex(db, "backupIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "num");

      OFileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) {
        Assert.assertTrue(backupDir.mkdirs());
      }

      final ExecutorService executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 4; i++) {
        Stack<CountDownLatch> producerIterationRecordCount = new Stack<>();
        producerIterationRecordCount.addAll(backupIterationRecordCount);
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.getAbsolutePath());

      youTrackDB.close();

      final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
      OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

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
      System.out.println("compare");

      boolean areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {
      if (youTrackDB != null && youTrackDB.isOpen()) {
        try {
          youTrackDB.close();
        } catch (Exception ex) {
          OLogManager.instance().error(this, "", ex);
        }
      }
      try {
        youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
        youTrackDB.drop(dbName);
        youTrackDB.drop(backupDbName);

        youTrackDB.close();

        OFileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
      }
    }
  }

  @Test
  public void testParallelBackupEncryption() throws Exception {
    backupIterationRecordCount.clear();
    for (int i = 0; i < 100; i++) {
      CountDownLatch latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }

    String testDirectory = DBTestBase.getDirectoryPath(getClass());
    OFileUtils.createDirectoryTree(testDirectory);

    final String backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = testDirectory + File.separator + backupDbName;
    final File backupDir = new File(testDirectory, "backupDir");

    dbName = StorageBackupMTTest.class.getSimpleName();
    String dbDirectory = testDirectory + File.separator + dbName;

    final YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();

    try {

      OFileUtils.deleteRecursively(new File(dbDirectory));

      youTrackDB = new YouTrackDB("embedded:" + testDirectory, config);

      youTrackDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

      var db = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

      final YTSchema schema = db.getMetadata().getSchema();
      final YTClass backupClass = schema.createClass("BackupClass");
      backupClass.createProperty(db, "num", YTType.INTEGER);
      backupClass.createProperty(db, "data", YTType.BINARY);

      backupClass.createIndex(db, "backupIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "num");

      OFileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) {
        Assert.assertTrue(backupDir.mkdirs());
      }

      final ExecutorService executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 4; i++) {
        Stack<CountDownLatch> producerIterationRecordCount = new Stack<>();
        producerIterationRecordCount.addAll(backupIterationRecordCount);
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.getAbsolutePath());

      youTrackDB.close();

      OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      YouTrackDBEmbedded embedded =
          (YouTrackDBEmbedded) YouTrackDBInternal.embedded(testDirectory, config);
      embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
      embedded.close();

      GlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
      youTrackDB = new YouTrackDB("embedded:" + testDirectory, YouTrackDBConfig.defaultConfig());
      final ODatabaseCompare compare =
          new ODatabaseCompare(
              (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin"),
              (YTDatabaseSessionInternal) youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);
      System.out.println("compare");

      boolean areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {
      if (youTrackDB.isOpen()) {
        try {
          youTrackDB.close();
        } catch (Exception ex) {
          OLogManager.instance().error(this, "", ex);
        }
      }
      try {
        youTrackDB = new YouTrackDB("embedded:" + testDirectory, config);
        youTrackDB.drop(dbName);
        youTrackDB.drop(backupDbName);

        youTrackDB.close();

        OFileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
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

        Random random = new Random();
        List<YTRID> ids = new ArrayList<>();
        while (!producerIterationRecordCount.isEmpty()) {

          for (int i = 0; i < count; i++) {
            try {
              db.begin();
              final byte[] data = new byte[random.nextInt(1024)];
              random.nextBytes(data);

              final int num = random.nextInt();
              if (!ids.isEmpty() && i % 8 == 0) {
                YTRID id = ids.remove(0);
                db.delete(id);
              } else if (!ids.isEmpty() && i % 4 == 0) {
                YTRID id = ids.remove(0);
                final EntityImpl document = db.load(id);
                document.field("data", data);
              } else {
                final EntityImpl document = new EntityImpl("BackupClass");
                document.field("num", num);
                document.field("data", data);

                document.save();
                YTRID id = document.getIdentity();
                if (ids.size() < 100) {
                  ids.add(id);
                }
              }
              db.commit();

            } catch (YTModificationOperationProhibitedException e) {
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
          CountDownLatch latch = backupIterationRecordCount.pop();
          latch.await();

          System.out.println(Thread.currentThread() + " do inc backup");
          db.incrementalBackup(backupPath);
          System.out.println(Thread.currentThread() + " done inc backup");
        }
      } catch (Exception | Error e) {
        OLogManager.instance().error(this, "", e);
        throw e;
      }
      finished.countDown();

      System.out.println(Thread.currentThread() + " - stop backup");

      return null;
    }
  }
}
