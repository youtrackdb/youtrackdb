/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.memory;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/24/14
 */
public final class ODirectMemoryOnlyDiskCache extends OAbstractWriteCache
    implements OReadCache, OWriteCache {
  private final Lock metadataLock = new ReentrantLock();

  private final Object2IntOpenHashMap<String> fileNameIdMap = new Object2IntOpenHashMap<>();
  private final Int2ObjectOpenHashMap<String> fileIdNameMap = new Int2ObjectOpenHashMap<>();

  private final ConcurrentMap<Integer, MemoryFile> files = new ConcurrentHashMap<>();

  private int counter;

  private final int pageSize;
  private final int id;

  ODirectMemoryOnlyDiskCache(final int pageSize, final int id) {
    this.pageSize = pageSize;
    this.id = id;
    fileNameIdMap.defaultReturnValue(-1);
  }

  /** {@inheritDoc} */
  @Override
  public Path getRootDirectory() {
    return null;
  }

  @Override
  public long addFile(final String fileName, final OWriteCache writeCache) {
    metadataLock.lock();
    try {
      var fileId = fileNameIdMap.getInt(fileName);

      if (fileId == -1) {
        counter++;
        final int id = counter;

        files.put(id, new MemoryFile(this.id, id));
        fileNameIdMap.put(fileName, id);

        fileId = id;

        fileIdNameMap.put(fileId, fileName);
      } else {
        throw new OStorageException(fileName + " already exists.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long fileIdByName(final String fileName) {
    metadataLock.lock();
    try {
      final int fileId = fileNameIdMap.getInt(fileName);
      if (fileId > -1) {
        return fileId;
      }
    } finally {
      metadataLock.unlock();
    }

    return -1;
  }

  @Override
  public final int internalFileId(final long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public final long externalFileId(final int fileId) {
    return composeFileId(id, fileId);
  }

  @Override
  public final long bookFileId(final String fileName) {
    metadataLock.lock();
    try {
      counter++;
      return composeFileId(id, counter);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void addBackgroundExceptionListener(final OBackgroundExceptionListener listener) {}

  @Override
  public final void removeBackgroundExceptionListener(
      final OBackgroundExceptionListener listener) {}

  @Override
  public final void checkCacheOverflow() {}

  @Override
  public final long addFile(
      final String fileName, final long fileId, final OWriteCache writeCache) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      if (files.containsKey(intId)) {
        throw new OStorageException("File with id " + intId + " already exists.");
      }

      if (fileNameIdMap.containsKey(fileName)) {
        throw new OStorageException(fileName + " already exists.");
      }

      files.put(intId, new MemoryFile(id, intId));
      fileNameIdMap.put(fileName, intId);
      fileIdNameMap.put(intId, fileName);

      return composeFileId(id, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final OCacheEntry loadForWrite(
      final long fileId,
      final long pageIndex,
      final OWriteCache writeCache,
      final boolean verifyChecksums,
      final OLogSequenceNumber startLSN) {
    assert fileId >= 0;
    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null) {
      return null;
    }

    cacheEntry.acquireExclusiveLock();
    return cacheEntry;
  }

  @Override
  public final OCacheEntry loadForRead(
      final long fileId,
      final long pageIndex,
      final OWriteCache writeCache,
      final boolean verifyChecksums) {

    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null) {
      return null;
    }

    cacheEntry.acquireSharedLock();

    return cacheEntry;
  }

  @Override
  public OCacheEntry silentLoadForRead(
      long extFileId, int pageIndex, OWriteCache writeCache, boolean verifyChecksums) {
    return loadForRead(extFileId, pageIndex, writeCache, verifyChecksums);
  }

  private OCacheEntry doLoad(final long fileId, final long pageIndex) {
    final int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    final OCacheEntry cacheEntry = memoryFile.loadPage(pageIndex);
    if (cacheEntry == null) {
      return null;
    }

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }

    return cacheEntry;
  }

  @Override
  public final OCacheEntry allocateNewPage(
      final long fileId, final OWriteCache writeCache, final OLogSequenceNumber startLSN) {
    final int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    final OCacheEntry cacheEntry = memoryFile.addNewPage(this);

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }
    cacheEntry.acquireExclusiveLock();
    return cacheEntry;
  }

  @Override
  public int allocateNewPage(final long fileId) {
    throw new UnsupportedOperationException();
  }

  private MemoryFile getFile(final int fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null) {
      throw new OStorageException("File with id " + fileId + " does not exist");
    }

    return memoryFile;
  }

  @Override
  public final void releaseFromWrite(
      final OCacheEntry cacheEntry, final OWriteCache writeCache, boolean changed) {
    cacheEntry.releaseExclusiveLock();

    doRelease(cacheEntry);
  }

  @Override
  public final void releaseFromRead(final OCacheEntry cacheEntry) {
    cacheEntry.releaseSharedLock();

    doRelease(cacheEntry);
  }

  private static void doRelease(final OCacheEntry cacheEntry) {
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (cacheEntry) {
      cacheEntry.decrementUsages();
      assert cacheEntry.getUsagesCount() > 0
          || cacheEntry.getCachePointer().getBuffer() == null
          || !cacheEntry.isLockAcquiredByCurrentThread();
    }
  }

  @Override
  public final long getFilledUpTo(final long fileId) {
    final int intId = extractFileId(fileId);
    final MemoryFile memoryFile = getFile(intId);
    return memoryFile.size();
  }

  @Override
  public final void flush(final long fileId) {}

  @Override
  public final void close(final long fileId, final boolean flush) {}

  @Override
  public final void deleteFile(final long fileId) {
    final int intId = extractFileId(fileId);
    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.remove(intId);
      if (fileName == null) {
        return;
      }

      fileNameIdMap.removeInt(fileName);
      final MemoryFile file = files.remove(intId);
      if (file != null) {
        file.clear();
      }
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void renameFile(final long fileId, final String newFileName) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.get(intId);
      if (fileName == null) {
        return;
      }

      fileNameIdMap.removeInt(fileName);

      fileIdNameMap.put(intId, newFileName);
      fileNameIdMap.put(newFileName, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void truncateFile(final long fileId) {
    final int intId = extractFileId(fileId);

    final MemoryFile file = getFile(intId);
    file.clear();
  }

  @Override
  public final void flush() {}

  @Override
  public final long[] close() {
    return new long[0];
  }

  @Override
  public final void clear() {
    delete();
  }

  @Override
  public final long[] delete() {
    metadataLock.lock();
    try {
      for (final MemoryFile file : files.values()) {
        file.clear();
      }

      files.clear();
      fileIdNameMap.clear();
      fileNameIdMap.clear();
    } finally {
      metadataLock.unlock();
    }

    return new long[0];
  }

  @Override
  public void replaceFileId(long fileId, long newFileId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void deleteStorage(final OWriteCache writeCache) {
    delete();
  }

  /** {@inheritDoc} */
  @Override
  public final void closeStorage(final OWriteCache writeCache) {
    //noinspection ResultOfMethodCallIgnored
    close();
  }

  @Override
  public void changeMaximumAmountOfMemory(final long calculateReadCacheMaxMemory) {}

  @Override
  public OPageDataVerificationError[] checkStoredPages(
      final OCommandOutputListener commandOutputListener) {
    return OCommonConst.EMPTY_PAGE_DATA_VERIFICATION_ARRAY;
  }

  @Override
  public boolean exists(final String name) {
    metadataLock.lock();
    try {
      final int fileId = fileNameIdMap.getInt(name);
      if (fileId == -1) {
        return false;
      }

      final MemoryFile memoryFile = files.get(fileId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final boolean exists(final long fileId) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final MemoryFile memoryFile = files.get(intId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void restoreModeOn() {}

  @Override
  public void restoreModeOff() {}

  @Override
  public final String fileNameById(final long fileId) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      return fileIdNameMap.get(intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final String nativeFileNameById(final long fileId) {
    return fileNameById(fileId);
  }

  @Override
  public final long getUsedMemory() {
    long totalPages = 0;
    for (final MemoryFile file : files.values()) {
      totalPages += file.getUsedMemory();
    }

    return totalPages * pageSize;
  }

  @Override
  public boolean checkLowDiskSpace() {
    return true;
  }

  /** Not implemented because has no sense */
  @Override
  public void addPageIsBrokenListener(final OPageIsBrokenListener listener) {}

  /** Not implemented because has no sense */
  @Override
  public void removePageIsBrokenListener(final OPageIsBrokenListener listener) {}

  @Override
  public long loadFile(final String fileName) {
    metadataLock.lock();
    try {
      final int fileId = fileNameIdMap.getInt(fileName);

      if (fileId == -1) {
        throw new OStorageException("File " + fileName + " does not exist.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final long addFile(final String fileName) {
    return addFile(fileName, null);
  }

  @Override
  public final long addFile(final String fileName, final long fileId) {
    return addFile(fileName, fileId, null);
  }

  @Override
  public void store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void syncDataFiles(final long segmentId, byte[] lastMetadata) {}

  @Override
  public void flushTillSegment(final long segmentId) {}

  @Override
  public Long getMinimalNotFlushedSegment() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDirtyPagesTable(
      final OCachePointer pointer, final OLogSequenceNumber startLSN) {}

  @Override
  public void create() {}

  @Override
  public void open() {}

  @Override
  public OCachePointer load(
      final long fileId,
      final long startPageIndex,
      final OModifiableBoolean cacheHit,
      final boolean verifyChecksums) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getExclusiveWriteCachePagesSize() {
    return 0;
  }

  @Override
  public void truncateFile(final long fileId, final OWriteCache writeCache) {
    truncateFile(fileId);
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public Map<String, Long> files() {
    final Object2LongOpenHashMap<String> result = new Object2LongOpenHashMap<>(1024);

    metadataLock.lock();
    try {
      for (final Object2IntMap.Entry<String> entry : fileNameIdMap.object2IntEntrySet()) {
        if (entry.getIntValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getIntValue()));
        }
      }
    } finally {
      metadataLock.unlock();
    }

    return result;
  }

  /**
   * @inheritDoc
   */
  @Override
  public final int pageSize() {
    return pageSize;
  }

  /**
   * @inheritDoc
   */
  @Override
  public final boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  @Override
  public final String restoreFileById(final long fileId) {
    return null;
  }

  @Override
  public final void closeFile(
      final long fileId, final boolean flush, final OWriteCache writeCache) {
    close(fileId, flush);
  }

  @Override
  public final void deleteFile(final long fileId, final OWriteCache writeCache) {
    deleteFile(fileId);
  }
}
