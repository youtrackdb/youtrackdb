package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v1;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.cache.ReadCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.ClusterPage;
import com.jetbrains.youtrack.db.internal.core.storage.disk.LocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.fs.File;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.AtomicUnitEndRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.AtomicUnitStartRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.FileCreatedWALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.LogSequenceNumber;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.NonTxOperationPerformedWALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.OperationUnitBodyRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.UpdatePageRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 8/27/13
 */
@Ignore
public class SBTreeV1WALTestIT extends SBTreeV1TestIT {

  static {
    GlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private LocalPaginatedStorage actualStorage;
  private WriteCache actualWriteCache;

  private DatabaseSession expectedDatabaseDocumentTx;
  private LocalPaginatedStorage expectedStorage;
  private ReadCache expectedReadCache;
  private WriteCache expectedWriteCache;

  private String expectedStorageDir;
  private String actualStorageDir;

  private static final String DIR_NAME = SBTreeV1WALTestIT.class.getSimpleName();
  private static final String ACTUAL_DB_NAME = "sbtreeV1WithWALTestActual";
  private static final String EXPECTED_DB_NAME = "sbtreeV1WithWALTestExpected";

  @Before
  public void before() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/" + DIR_NAME;

    final java.io.File buildDir = new java.io.File(buildDirectory);
    FileUtils.deleteRecursively(buildDir);

    youTrackDB = new YouTrackDBImpl("plocal:" + buildDir, YouTrackDBConfig.defaultConfig());
    storage =
        (AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();
    createExpectedSBTree();
    createActualSBTree();
  }

  @After
  @Override
  public void afterMethod() throws Exception {
    youTrackDB.drop(ACTUAL_DB_NAME);
    youTrackDB.drop(EXPECTED_DB_NAME);
    youTrackDB.close();
  }

  private void createActualSBTree() throws Exception {
    youTrackDB.execute(
        "create database "
            + ACTUAL_DB_NAME
            + " plocal users ( admin identified by 'admin' role admin)");

    databaseDocumentTx = youTrackDB.open(ACTUAL_DB_NAME, "admin", "admin");
    actualStorage =
        (LocalPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();
    actualStorageDir = actualStorage.getStoragePath().toString();
    CASDiskWriteAheadLog writeAheadLog = (CASDiskWriteAheadLog) actualStorage.getWALInstance();

    actualStorage.synch();
    writeAheadLog.addCutTillLimit(writeAheadLog.getFlushedLsn());

    actualWriteCache = actualStorage.getWriteCache();
    atomicOperationsManager = actualStorage.getAtomicOperationsManager();

    sbTree = new SBTreeV1<>("actualSBTree", ".sbt", ".nbt", actualStorage);
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(
                atomicOperation,
                IntegerSerializer.INSTANCE,
                LinkSerializer.INSTANCE,
                null,
                1,
                false
            ));
  }

  private void createExpectedSBTree() {
    youTrackDB.execute(
        "create database "
            + EXPECTED_DB_NAME
            + " plocal users ( admin identified by 'admin' role admin)");

    expectedDatabaseDocumentTx = youTrackDB.open(EXPECTED_DB_NAME, "admin", "admin");
    expectedStorage =
        (LocalPaginatedStorage)
            ((DatabaseSessionInternal) expectedDatabaseDocumentTx).getStorage();
    expectedReadCache = expectedStorage.getReadCache();
    expectedWriteCache = expectedStorage.getWriteCache();

    expectedStorageDir = expectedStorage.getStoragePath().toString();
  }

  @Override
  @Test
  public void testKeyPut() throws Exception {
    super.testKeyPut();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomUniform() throws Exception {
    super.testKeyPutRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    super.testKeyPutRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    super.testKeyDeleteRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDeleteRandomGaussian() throws Exception {
    super.testKeyDeleteRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDelete() throws Exception {
    super.testKeyDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyAddDelete() throws Exception {
    super.testKeyAddDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeFirstEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeLastEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    super.testAddKeyValuesAndRemoveFirstMiddleAndLastPages();

    assertFileRestoreFromWAL();
  }

  @Test
  @Ignore
  @Override
  public void testNullKeysInSBTree() throws Exception {
    super.testNullKeysInSBTree();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMajor() throws Exception {
    super.testIterateEntriesMajor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMinor() throws Exception {
    super.testIterateEntriesMinor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesBetween() throws Exception {
    super.testIterateEntriesBetween();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    long sbTreeFileId = actualWriteCache.fileIdByName(sbTree.getName() + ".sbt");
    String nativeSBTreeFileName = actualWriteCache.nativeFileNameById(sbTreeFileId);

    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();
    actualStorage.shutdown();

    restoreDataFromWAL();

    long expectedSBTreeFileId = expectedWriteCache.fileIdByName("expectedSBTree.sbt");
    String expectedSBTreeNativeFileName =
        expectedWriteCache.nativeFileNameById(expectedSBTreeFileId);

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.close();
    expectedStorage.shutdown();

    assertFileContentIsTheSame(expectedSBTreeNativeFileName, nativeSBTreeFileName);
  }

  private void restoreDataFromWAL() throws IOException {
    CASDiskWriteAheadLog log =
        new CASDiskWriteAheadLog(
            ACTUAL_DB_NAME,
            Paths.get(actualStorageDir),
            Paths.get(actualStorageDir),
            10_000,
            128,
            null,
            null,
            30 * 60 * 1_000_000_000L,
            100 * 1024 * 1024,
            1000,
            false,
            Locale.ENGLISH,
            -1,
            1_000,
            false,
            true,
            false,
            0);
    LogSequenceNumber lsn = log.begin();

    List<WALRecord> atomicUnit = new ArrayList<>();
    List<WriteableWALRecord> walRecords = log.read(lsn, 1_000);

    boolean atomicChangeIsProcessed = false;
    while (!walRecords.isEmpty()) {
      for (WriteableWALRecord walRecord : walRecords) {
        if (walRecord instanceof OperationUnitBodyRecord) {
          atomicUnit.add(walRecord);
        }

        if (!atomicChangeIsProcessed) {
          if (walRecord instanceof AtomicUnitStartRecord) {
            atomicChangeIsProcessed = true;
          }
        } else if (walRecord instanceof AtomicUnitEndRecord) {
          atomicChangeIsProcessed = false;

          for (WALRecord restoreRecord : atomicUnit) {
            if (restoreRecord instanceof AtomicUnitStartRecord
                || restoreRecord instanceof AtomicUnitEndRecord
                || restoreRecord instanceof NonTxOperationPerformedWALRecord) {
              continue;
            }

            if (restoreRecord instanceof FileCreatedWALRecord fileCreatedCreatedRecord) {
              final String fileName =
                  fileCreatedCreatedRecord.getFileName().replace("actualSBTree", "expectedSBTree");

              if (!expectedWriteCache.exists(fileName)) {
                expectedReadCache.addFile(
                    fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
              }
            } else {
              final UpdatePageRecord updatePageRecord = (UpdatePageRecord) restoreRecord;

              final long fileId = updatePageRecord.getFileId();
              final long pageIndex = updatePageRecord.getPageIndex();

              if (!expectedWriteCache.exists(fileId)) {
                // some files can be absent for example configuration files
                continue;
              }

              CacheEntry cacheEntry =
                  expectedReadCache.loadForWrite(
                      fileId, pageIndex, expectedWriteCache, false, null);
              if (cacheEntry == null) {
                do {
                  if (cacheEntry != null) {
                    expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache, true);
                  }

                  cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, null);
                } while (cacheEntry.getPageIndex() != pageIndex);
              }

              try {
                DurablePage durablePage = new DurablePage(cacheEntry);
                durablePage.restoreChanges(updatePageRecord.getChanges());
                durablePage.setLsn(new LogSequenceNumber(0, 0));
              } finally {
                expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache, true);
              }
            }
          }
          atomicUnit.clear();
        } else {
          Assert.assertTrue(
              "WAL record type is " + walRecord.getClass().getName(),
              walRecord instanceof UpdatePageRecord
                  || walRecord instanceof NonTxOperationPerformedWALRecord
                  || walRecord instanceof FileCreatedWALRecord);
        }
      }

      walRecords = log.next(walRecords.get(walRecords.size() - 1).getLsn(), 1_000);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertFileContentIsTheSame(String expectedBTreeFileName, String actualBTreeFileName)
      throws IOException {
    java.io.File expectedFile = new java.io.File(expectedStorageDir, expectedBTreeFileName);
    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile fileTwo =
          new RandomAccessFile(new java.io.File(actualStorageDir, actualBTreeFileName), "r")) {

        Assert.assertEquals(fileOne.length(), fileTwo.length());

        byte[] expectedContent = new byte[ClusterPage.PAGE_SIZE];
        byte[] actualContent = new byte[ClusterPage.PAGE_SIZE];

        fileOne.seek(File.HEADER_SIZE);
        fileTwo.seek(File.HEADER_SIZE);

        int bytesRead = fileOne.read(expectedContent);
        while (bytesRead >= 0) {
          fileTwo.readFully(actualContent, 0, bytesRead);

          Assertions.assertThat(
                  Arrays.copyOfRange(
                      expectedContent,
                      DurablePage.NEXT_FREE_POSITION,
                      DurablePage.MAX_PAGE_SIZE_BYTES))
              .isEqualTo(
                  Arrays.copyOfRange(
                      actualContent,
                      DurablePage.NEXT_FREE_POSITION,
                      DurablePage.MAX_PAGE_SIZE_BYTES));
          expectedContent = new byte[ClusterPage.PAGE_SIZE];
          actualContent = new byte[ClusterPage.PAGE_SIZE];
          bytesRead = fileOne.read(expectedContent);
        }
      }
    }
  }
}
