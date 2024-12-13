package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v3;

import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
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
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.NonTxOperationPerformedWALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.FileCreatedWALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.LogSequenceNumber;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.OperationUnitBodyRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.UpdatePageRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.MurmurHash3HashFunction;
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

/**
 * @since 5/19/14
 */
public class LocalHashTableV3WALTestIT extends LocalHashTableV3Base {

  private static final String ACTUAL_DB_NAME =
      LocalHashTableV3WALTestIT.class.getSimpleName() + "Actual";
  private static final String EXPECTED_DB_NAME =
      LocalHashTableV3WALTestIT.class.getSimpleName() + "Expected";

  private LocalPaginatedStorage actualStorage;
  private LocalPaginatedStorage expectedStorage;

  private String actualStorageDir;
  private String expectedStorageDir;

  private DatabaseSession databaseDocumentTx;

  private WriteCache actualWriteCache;

  private DatabaseSession expectedDatabaseDocumentTx;
  private WriteCache expectedWriteCache;

  private YouTrackDB youTrackDB;

  @Before
  public void before() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/" + this.getClass().getSimpleName();

    final java.io.File buildDir = new java.io.File(buildDirectory);
    FileUtils.deleteRecursively(buildDir);

    youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    CreateDatabaseUtil.createDatabase(ACTUAL_DB_NAME, youTrackDB, CreateDatabaseUtil.TYPE_PLOCAL);
    // youTrackDB.create(ACTUAL_DB_NAME, DatabaseType.PLOCAL);
    databaseDocumentTx =
        youTrackDB.open(ACTUAL_DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    CreateDatabaseUtil.createDatabase(EXPECTED_DB_NAME, youTrackDB,
        CreateDatabaseUtil.TYPE_PLOCAL);
    // youTrackDB.create(EXPECTED_DB_NAME, DatabaseType.PLOCAL);
    expectedDatabaseDocumentTx =
        youTrackDB.open(EXPECTED_DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    expectedStorage =
        ((LocalPaginatedStorage)
            ((DatabaseSessionInternal) expectedDatabaseDocumentTx).getStorage());
    actualStorage =
        (LocalPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();

    atomicOperationsManager = actualStorage.getAtomicOperationsManager();

    actualStorageDir = actualStorage.getStoragePath().toString();
    expectedStorageDir = expectedStorage.getStoragePath().toString();

    actualWriteCache =
        ((LocalPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage())
            .getWriteCache();
    expectedWriteCache =
        ((LocalPaginatedStorage)
            ((DatabaseSessionInternal) expectedDatabaseDocumentTx).getStorage())
            .getWriteCache();

    CASDiskWriteAheadLog diskWriteAheadLog = (CASDiskWriteAheadLog) actualStorage.getWALInstance();

    actualStorage.synch();
    diskWriteAheadLog.addCutTillLimit(diskWriteAheadLog.getFlushedLsn());

    createActualHashTable();
  }

  @After
  public void after() {
    youTrackDB.drop(ACTUAL_DB_NAME);
    youTrackDB.drop(EXPECTED_DB_NAME);
    youTrackDB.close();
  }

  private void createActualHashTable() throws IOException {
    MurmurHash3HashFunction<Integer> murmurHash3HashFunction =
        new MurmurHash3HashFunction<>(IntegerSerializer.INSTANCE);

    localHashTable =
        new LocalHashTableV3<>(
            "actualLocalHashTable",
            ".imc",
            ".tsc",
            ".obf",
            ".nbh",
            (AbstractPaginatedStorage)
                ((DatabaseSessionInternal) databaseDocumentTx).getStorage());
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localHashTable.create(
                atomicOperation,
                IntegerSerializer.INSTANCE,
                BinarySerializerFactory.getInstance().getObjectSerializer(PropertyType.STRING),
                null,
                null,
                murmurHash3HashFunction,
                true));
  }

  @Override
  public void testKeyPut() throws IOException {
    super.testKeyPut();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomUniform() throws IOException {
    super.testKeyPutRandomUniform();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomGaussian() throws IOException {
    super.testKeyPutRandomGaussian();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDelete() throws IOException {
    super.testKeyDelete();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomGaussian() throws IOException {
    super.testKeyDeleteRandomGaussian();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyAddDelete() throws IOException {
    super.testKeyAddDelete();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRemoveNullKey() throws IOException {
    super.testKeyPutRemoveNullKey();

    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    final long imcFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".imc");
    final String nativeImcFileName = actualWriteCache.nativeFileNameById(imcFileId);

    final long tscFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".tsc");
    final String nativeTscFileName = actualWriteCache.nativeFileNameById(tscFileId);

    final long nbhFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".nbh");
    final String nativeNBHFileName = actualWriteCache.nativeFileNameById(nbhFileId);

    final long obfFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".obf");
    final String nativeOBFFileName = actualWriteCache.nativeFileNameById(obfFileId);

    localHashTable.close();

    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();
    actualStorage.shutdown();

    System.out.println("Start data restore");
    restoreDataFromWAL();
    System.out.println("Stop data restore");

    final long expectedImcFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.imc");
    final String nativeExpectedImcFileName =
        expectedWriteCache.nativeFileNameById(expectedImcFileId);

    final long expectedTscFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.tsc");
    final String nativeExpectedTscFileName =
        expectedWriteCache.nativeFileNameById(expectedTscFileId);

    final long expectedNbhFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.nbh");
    final String nativeExpectedNBHFileName =
        expectedWriteCache.nativeFileNameById(expectedNbhFileId);

    final long expectedObfFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.obf");
    final String nativeExpectedOBFFile = expectedWriteCache.nativeFileNameById(expectedObfFileId);

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.close();
    expectedStorage.shutdown();

    System.out.println("Start data comparison");

    assertFileContentIsTheSame(
        nativeExpectedImcFileName,
        nativeImcFileName,
        nativeExpectedTscFileName,
        nativeTscFileName,
        nativeExpectedNBHFileName,
        nativeNBHFileName,
        nativeExpectedOBFFile,
        nativeOBFFileName);

    System.out.println("Stop data comparison");
  }

  private void restoreDataFromWAL() throws IOException {
    final ReadCache expectedReadCache =
        ((AbstractPaginatedStorage)
            ((DatabaseSessionInternal) expectedDatabaseDocumentTx).getStorage())
            .getReadCache();

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
                  fileCreatedCreatedRecord
                      .getFileName()
                      .replace("actualLocalHashTable", "expectedLocalHashTable");

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

  private void assertFileContentIsTheSame(
      String expectedIMCFile,
      String actualIMCFile,
      String expectedTSCFile,
      String actualTSCFile,
      String expectedNBHFile,
      String actualNBHFile,
      String expectedOBFFile,
      String actualOBFFile)
      throws IOException {

    assertFileContentIsTheSame(
        new java.io.File(expectedStorageDir, expectedIMCFile).getAbsolutePath(),
        new java.io.File(actualStorageDir, actualIMCFile).getAbsolutePath());
    assertFileContentIsTheSame(
        new java.io.File(expectedStorageDir, expectedTSCFile).getAbsolutePath(),
        new java.io.File(actualStorageDir, actualTSCFile).getAbsolutePath());
    assertFileContentIsTheSame(
        new java.io.File(expectedStorageDir, expectedNBHFile).getAbsolutePath(),
        new java.io.File(actualStorageDir, actualNBHFile).getAbsolutePath());
    assertFileContentIsTheSame(
        new java.io.File(expectedStorageDir, expectedOBFFile).getAbsolutePath(),
        new java.io.File(actualStorageDir, actualOBFFile).getAbsolutePath());
  }

  private static void assertFileContentIsTheSame(
      String expectedBTreeFileName, String actualBTreeFileName) throws IOException {
    java.io.File expectedFile = new java.io.File(expectedBTreeFileName);
    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile fileTwo =
          new RandomAccessFile(new java.io.File(actualBTreeFileName), "r")) {

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
