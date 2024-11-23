/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileCreatedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileDeletedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Note: all atomic operations methods are designed in context that all operations on single files
 * will be wrapped in shared lock.
 *
 * @since 12/3/13
 */
final class OAtomicOperationBinaryTracking implements OAtomicOperation {

  private final int storageId;
  private final long operationUnitId;

  private boolean rollback;

  private final Set<String> lockedObjects = new HashSet<>();
  private final Long2ObjectOpenHashMap<FileChanges> fileChanges = new Long2ObjectOpenHashMap<>();
  private final Object2LongOpenHashMap<String> newFileNamesId = new Object2LongOpenHashMap<>();
  private final LongOpenHashSet deletedFiles = new LongOpenHashSet();
  private final Object2LongOpenHashMap<String> deletedFileNameIdMap =
      new Object2LongOpenHashMap<>();

  private final OReadCache readCache;
  private final OWriteCache writeCache;

  private final Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<>();

  private int componentOperationsCount;

  /**
   * Pointers to ridbags deleted during current transaction. We can not reuse pointers if we delete
   * ridbag and then create new one inside of the same transaction.
   */
  private final Set<OBonsaiBucketPointer> deletedBonsaiPointers = new HashSet<>();

  private final Map<IntIntImmutablePair, IntSet> deletedRecordPositions = new HashMap<>();

  OAtomicOperationBinaryTracking(
      final long operationUnitId,
      final OReadCache readCache,
      final OWriteCache writeCache,
      final int storageId) {
    newFileNamesId.defaultReturnValue(-1);
    deletedFileNameIdMap.defaultReturnValue(-1);

    this.storageId = storageId;
    this.operationUnitId = operationUnitId;

    this.readCache = readCache;
    this.writeCache = writeCache;
  }

  @Override
  public long getOperationUnitId() {
    return operationUnitId;
  }

  @Override
  public OCacheEntry loadPageForWrite(
      long fileId, final long pageIndex, final int pageCount, final boolean verifyChecksum)
      throws IOException {
    assert pageCount > 0;
    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId)) {
      throw new OStorageException("File with id " + fileId + " is deleted.");
    }
    final FileChanges changesContainer =
        fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex) {
        return changesContainer.pageChangesMap.get(pageIndex);
      } else {
        return null;
      }
    } else {
      OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);
      if (checkChangesFilledUpTo(changesContainer, pageIndex)) {
        if (pageChangesContainer == null) {
          final OCacheEntry delegate =
              readCache.loadForRead(fileId, pageIndex, writeCache, verifyChecksum);
          if (delegate != null) {
            pageChangesContainer = new OCacheEntryChanges(verifyChecksum, this);
            changesContainer.pageChangesMap.put(pageIndex, pageChangesContainer);
            pageChangesContainer.delegate = delegate;
            return pageChangesContainer;
          }
        } else {
          if (pageChangesContainer.isNew) {
            return pageChangesContainer;
          } else {
            // Need to load the page again from cache for locking reasons
            pageChangesContainer.delegate =
                readCache.loadForRead(fileId, pageIndex, writeCache, verifyChecksum);
            return pageChangesContainer;
          }
        }
      }
    }
    return null;
  }

  @Override
  public OCacheEntry loadPageForRead(long fileId, final long pageIndex) throws IOException {

    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId)) {
      throw new OStorageException("File with id " + fileId + " is deleted.");
    }

    final FileChanges changesContainer = fileChanges.get(fileId);
    if (changesContainer == null) {
      return readCache.loadForRead(fileId, pageIndex, writeCache, true);
    }

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex) {
        return changesContainer.pageChangesMap.get(pageIndex);
      } else {
        return null;
      }
    } else {
      final OCacheEntryChanges pageChangesContainer =
          changesContainer.pageChangesMap.get(pageIndex);

      if (checkChangesFilledUpTo(changesContainer, pageIndex)) {
        if (pageChangesContainer == null) {
          return readCache.loadForRead(fileId, pageIndex, writeCache, true);
        } else {
          if (pageChangesContainer.isNew) {
            return pageChangesContainer;
          } else {
            // Need to load the page again from cache for locking reasons
            pageChangesContainer.delegate =
                readCache.loadForRead(fileId, pageIndex, writeCache, true);
            return pageChangesContainer;
          }
        }
      }
    }
    return null;
  }

  /**
   * Add metadata with given key inside of atomic operation. If metadata with the same key insist
   * inside of atomic operation it will be overwritten.
   *
   * @param metadata Metadata to add.
   * @see OAtomicOperationMetadata
   */
  @Override
  public void addMetadata(final OAtomicOperationMetadata<?> metadata) {
    this.metadata.put(metadata.getKey(), metadata);
  }

  /**
   * @param key Key of metadata which is looking for.
   * @return Metadata by associated key or <code>null</code> if such metadata is absent.
   */
  @Override
  public OAtomicOperationMetadata<?> getMetadata(final String key) {
    return metadata.get(key);
  }

  /**
   * @return All keys and associated metadata contained inside of atomic operation
   */
  private Map<String, OAtomicOperationMetadata<?>> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }

  @Override
  public void addDeletedRidBag(OBonsaiBucketPointer rootPointer) {
    deletedBonsaiPointers.add(rootPointer);
  }

  @Override
  public Set<OBonsaiBucketPointer> getDeletedBonsaiPointers() {
    return deletedBonsaiPointers;
  }

  @Override
  public OCacheEntry addPage(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId)) {
      throw new OStorageException("File with id " + fileId + " is deleted.");
    }

    final FileChanges changesContainer =
        fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    final long filledUpTo = internalFilledUpTo(fileId, changesContainer);

    OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(filledUpTo);
    assert pageChangesContainer == null;

    pageChangesContainer = new OCacheEntryChanges(false, this);
    pageChangesContainer.isNew = true;

    changesContainer.pageChangesMap.put(filledUpTo, pageChangesContainer);
    changesContainer.maxNewPageIndex = filledUpTo;
    pageChangesContainer.delegate =
        new OCacheEntryImpl(
            fileId,
            (int) filledUpTo,
            new OCachePointer(null, null, fileId, (int) filledUpTo),
            false,
            readCache);
    return pageChangesContainer;
  }

  @Override
  public void releasePageFromRead(final OCacheEntry cacheEntry) {
    if (cacheEntry instanceof OCacheEntryChanges) {
      releasePageFromWrite(cacheEntry);
    } else {
      readCache.releaseFromRead(cacheEntry);
    }
  }

  @Override
  public void releasePageFromWrite(final OCacheEntry cacheEntry) {
    final OCacheEntryChanges real = (OCacheEntryChanges) cacheEntry;

    if (deletedFiles.contains(cacheEntry.getFileId())) {
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");
    }

    if (cacheEntry.getCachePointer().getBuffer() != null) {
      readCache.releaseFromRead(real.getDelegate());
    } else {
      assert real.isNew || !cacheEntry.isLockAcquiredByCurrentThread();
    }
  }

  @Override
  public long filledUpTo(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);
    if (deletedFiles.contains(fileId)) {
      throw new OStorageException("File with id " + fileId + " is deleted.");
    }
    final FileChanges changesContainer = fileChanges.get(fileId);
    return internalFilledUpTo(fileId, changesContainer);
  }

  private long internalFilledUpTo(final long fileId, FileChanges changesContainer) {
    if (changesContainer == null) {
      changesContainer = new FileChanges();
      fileChanges.put(fileId, changesContainer);
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -2) {
      return changesContainer.maxNewPageIndex + 1;
    } else if (changesContainer.truncate) {
      return 0;
    }

    return writeCache.getFilledUpTo(fileId);
  }

  /**
   * This check if a file was trimmed or trunked in the current atomic operation.
   *
   * @param changesContainer changes container to check
   * @param pageIndex        limit to check against the changes
   * @return true if there are no changes or pageIndex still fit, false if the pageIndex do not fit
   * anymore
   */
  private static boolean checkChangesFilledUpTo(
      final FileChanges changesContainer, final long pageIndex) {
    if (changesContainer == null) {
      return true;
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -2) {
      return pageIndex < changesContainer.maxNewPageIndex + 1;
    } else {
      return !changesContainer.truncate;
    }
  }

  @Override
  public long addFile(final String fileName) {
    if (newFileNamesId.containsKey(fileName)) {
      throw new OStorageException("File with name " + fileName + " already exists.");
    }
    final long fileId;
    final boolean isNew;

    if (deletedFileNameIdMap.containsKey(fileName)) {
      fileId = deletedFileNameIdMap.removeLong(fileName);
      deletedFiles.remove(fileId);
      isNew = false;
    } else {
      fileId = writeCache.bookFileId(fileName);
      isNew = true;
    }
    newFileNamesId.put(fileName, fileId);

    final FileChanges fileChanges = new FileChanges();
    fileChanges.isNew = isNew;
    fileChanges.fileName = fileName;
    fileChanges.maxNewPageIndex = -1;

    this.fileChanges.put(fileId, fileChanges);

    return fileId;
  }

  @Override
  public long loadFile(final String fileName) throws IOException {
    long fileId = newFileNamesId.getLong(fileName);
    if (fileId == -1) {
      fileId = writeCache.loadFile(fileName);
    }
    this.fileChanges.computeIfAbsent(fileId, k -> new FileChanges());
    return fileId;
  }

  @Override
  public void deleteFile(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    final FileChanges fileChanges = this.fileChanges.remove(fileId);
    if (fileChanges != null && fileChanges.fileName != null) {
      newFileNamesId.removeLong(fileChanges.fileName);
    } else {
      deletedFiles.add(fileId);
      final String f = writeCache.fileNameById(fileId);
      if (f != null) {
        deletedFileNameIdMap.put(f, fileId);
      }
    }
  }

  @Override
  public boolean isFileExists(final String fileName) {
    if (newFileNamesId.containsKey(fileName)) {
      return true;
    }

    if (deletedFileNameIdMap.containsKey(fileName)) {
      return false;
    }

    return writeCache.exists(fileName);
  }

  @Override
  public String fileNameById(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    final FileChanges fileChanges = this.fileChanges.get(fileId);

    if (fileChanges != null && fileChanges.fileName != null) {
      return fileChanges.fileName;
    }

    if (deletedFiles.contains(fileId)) {
      throw new OStorageException("File with id " + fileId + " was deleted.");
    }

    return writeCache.fileNameById(fileId);
  }

  @Override
  public long fileIdByName(final String fileName) {
    long fileId = newFileNamesId.getLong(fileName);
    if (fileId > -1) {
      return fileId;
    }

    if (deletedFileNameIdMap.containsKey(fileName)) {
      return -1;
    }

    return writeCache.fileIdByName(fileName);
  }

  @Override
  public void truncateFile(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    final FileChanges fileChanges =
        this.fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    fileChanges.pageChangesMap.clear();
    fileChanges.maxNewPageIndex = -1;

    if (fileChanges.isNew) {
      return;
    }

    fileChanges.truncate = true;
  }

  public OLogSequenceNumber commitChanges(final OWriteAheadLog writeAheadLog) throws IOException {
    OLogSequenceNumber txEndLsn = null;

    final OLogSequenceNumber startLSN = writeAheadLog.end();

    var deletedFilesIterator = deletedFiles.longIterator();
    while (deletedFilesIterator.hasNext()) {
      final long deletedFileId = deletedFilesIterator.nextLong();
      writeAheadLog.log(new OFileDeletedWALRecord(operationUnitId, deletedFileId));
    }

    for (final Long2ObjectMap.Entry<FileChanges> fileChangesEntry :
        fileChanges.long2ObjectEntrySet()) {
      final FileChanges fileChanges = fileChangesEntry.getValue();
      final long fileId = fileChangesEntry.getLongKey();

      if (fileChanges.isNew) {
        writeAheadLog.log(new OFileCreatedWALRecord(operationUnitId, fileChanges.fileName, fileId));
      } else if (fileChanges.truncate) {
        OLogManager.instance()
            .warn(
                this,
                "You performing truncate operation which is considered unsafe because can not be"
                    + " rolled back, as result data can be incorrectly restored after crash, this"
                    + " operation is not recommended to be used");
      }

      final Iterator<Long2ObjectMap.Entry<OCacheEntryChanges>> filePageChangesIterator =
          fileChanges.pageChangesMap.long2ObjectEntrySet().iterator();
      while (filePageChangesIterator.hasNext()) {
        final Long2ObjectMap.Entry<OCacheEntryChanges> filePageChangesEntry =
            filePageChangesIterator.next();

        if (filePageChangesEntry.getValue().changes.hasChanges()) {
          final long pageIndex = filePageChangesEntry.getLongKey();
          final OCacheEntryChanges filePageChanges = filePageChangesEntry.getValue();

          final OLogSequenceNumber initialLSN = filePageChanges.getInitialLSN();
          Objects.requireNonNull(initialLSN);
          final OUpdatePageRecord updatePageRecord =
              new OUpdatePageRecord(
                  pageIndex, fileId, operationUnitId, filePageChanges.changes, initialLSN);
          writeAheadLog.log(updatePageRecord);
          filePageChanges.setChangeLSN(updatePageRecord.getLsn());

        } else {
          filePageChangesIterator.remove();
        }
      }
    }

    txEndLsn =
        writeAheadLog.log(new OAtomicUnitEndRecord(operationUnitId, rollback, getMetadata()));

    deletedFilesIterator = deletedFiles.longIterator();
    while (deletedFilesIterator.hasNext()) {
      var deletedFileId = deletedFilesIterator.nextLong();
      readCache.deleteFile(deletedFileId, writeCache);
    }

    for (final Long2ObjectMap.Entry<FileChanges> fileChangesEntry :
        fileChanges.long2ObjectEntrySet()) {
      final FileChanges fileChanges = fileChangesEntry.getValue();
      final long fileId = fileChangesEntry.getLongKey();

      if (fileChanges.isNew) {
        readCache.addFile(
            fileChanges.fileName, newFileNamesId.getLong(fileChanges.fileName), writeCache);
      } else if (fileChanges.truncate) {
        OLogManager.instance()
            .warn(
                this,
                "You performing truncate operation which is considered unsafe because can not be"
                    + " rolled back, as result data can be incorrectly restored after crash, this"
                    + " operation is not recommended to be used");
        readCache.truncateFile(fileId, writeCache);
      }

      final Iterator<Long2ObjectMap.Entry<OCacheEntryChanges>> filePageChangesIterator =
          fileChanges.pageChangesMap.long2ObjectEntrySet().iterator();
      while (filePageChangesIterator.hasNext()) {
        final Long2ObjectMap.Entry<OCacheEntryChanges> filePageChangesEntry =
            filePageChangesIterator.next();

        if (filePageChangesEntry.getValue().changes.hasChanges()) {
          final long pageIndex = filePageChangesEntry.getLongKey();
          final OCacheEntryChanges filePageChanges = filePageChangesEntry.getValue();

          OCacheEntry cacheEntry =
              readCache.loadForWrite(
                  fileId, pageIndex, writeCache, filePageChanges.verifyCheckSum, startLSN);
          if (cacheEntry == null) {
            if (!filePageChanges.isNew) {
              throw new OStorageException(
                  "Page with index " + pageIndex + " is not found in file with id " + fileId);
            }
            do {
              if (cacheEntry != null) {
                readCache.releaseFromWrite(cacheEntry, writeCache, true);
              }

              cacheEntry = readCache.allocateNewPage(fileId, writeCache, startLSN);
            } while (cacheEntry.getPageIndex() != pageIndex);
          }

          try {
            final ODurablePage durablePage = new ODurablePage(cacheEntry);
            cacheEntry.setEndLSN(txEndLsn);

            durablePage.restoreChanges(filePageChanges.changes);
            durablePage.setLsn(filePageChanges.getChangeLSN());
          } finally {
            readCache.releaseFromWrite(cacheEntry, writeCache, true);
          }
        } else {
          filePageChangesIterator.remove();
        }
      }
    }

    return txEndLsn;
  }

  public void rollbackInProgress() {
    rollback = true;
  }

  public boolean isRollbackInProgress() {
    return rollback;
  }

  public void addLockedObject(final String lockedObject) {
    lockedObjects.add(lockedObject);
  }

  public boolean containsInLockedObjects(final String objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  public Iterable<String> lockedObjects() {
    return lockedObjects;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OAtomicOperationBinaryTracking operation = (OAtomicOperationBinaryTracking) o;

    return operationUnitId == operation.operationUnitId;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(operationUnitId);
  }

  private static final class FileChanges {

    private final Long2ObjectOpenHashMap<OCacheEntryChanges> pageChangesMap =
        new Long2ObjectOpenHashMap<>();
    private long maxNewPageIndex = -2;
    private boolean isNew;
    private boolean truncate;
    private String fileName;
  }

  private static int storageId(final long fileId) {
    return (int) (fileId >>> 32);
  }

  private static long composeFileId(final long fileId, final int storageId) {
    return (((long) storageId) << 32) | fileId;
  }

  private static long checkFileIdCompatibility(final long fileId, final int storageId) {
    // indicates that storage has no it's own id.
    if (storageId == -1) {
      return fileId;
    }
    if (storageId(fileId) == 0) {
      return composeFileId(fileId, storageId);
    }
    return fileId;
  }

  @Override
  public void addDeletedRecordPosition(int clusterId, int pageIndex, int recordPosition) {
    var key = new IntIntImmutablePair(clusterId, pageIndex);
    final IntSet recordPositions =
        deletedRecordPositions.computeIfAbsent(key, k -> new IntOpenHashSet());
    recordPositions.add(recordPosition);
  }

  @Override
  public IntSet getBookedRecordPositions(int clusterId, int pageIndex) {
    return deletedRecordPositions.getOrDefault(
        new IntIntImmutablePair(clusterId, pageIndex), IntSets.emptySet());
  }

  @Override
  public void incrementComponentOperations() {
    componentOperationsCount++;
  }

  @Override
  public void decrementComponentOperations() {
    componentOperationsCount--;
  }

  @Override
  public int getComponentOperations() {
    return componentOperationsCount;
  }
}
