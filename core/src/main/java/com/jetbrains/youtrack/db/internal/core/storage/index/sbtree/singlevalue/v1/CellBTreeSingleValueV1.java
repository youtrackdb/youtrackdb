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

package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.v1;

import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.exception.TooBigIndexKeyException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.comparator.AlwaysGreaterKey;
import com.jetbrains.youtrack.db.internal.core.index.comparator.AlwaysLessKey;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.CellBTreeSingleValue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This is implementation which is based on B+-tree implementation threaded tree. The main
 * differences are:
 *
 * <ol>
 *   <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused
 *       later when new items are added.
 *   <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more
 *       than keys contained in current bucket
 *       <ol/>
 *         There is support of null values for keys, but values itself cannot be null. Null keys
 *         support is switched off by default if null keys are supported value which is related to
 *         null key will be stored in separate file which has only one page. Buckets/pages for usual
 *         (non-null) key-value entries can be considered as sorted array. The first bytes of page
 *         contains such auxiliary information as size of entries contained in bucket, links to
 *         neighbors which contain entries with keys less/more than keys in current bucket. The next
 *         bytes contain sorted array of entries. Array itself is split on two parts. First part is
 *         growing from start to end, and second part is growing from end to start. First part is
 *         array of offsets to real key-value entries which are stored in second part of array which
 *         grows from end to start. This array of offsets is sorted by accessing order according to
 *         key value. So we can use binary search to find requested key. When new key-value pair is
 *         added we append binary presentation of this pair to the second part of array which grows
 *         from end of page to start, remember value of offset for this pair, and find proper
 *         position of this offset inside of first part of array. Such approach allows to minimize
 *         amount of memory involved in performing of operations and as result speed up data
 *         processing.
 *
 * @since 8/7/13
 */
public final class CellBTreeSingleValueV1<K> extends DurableComponent
    implements CellBTreeSingleValue<K> {

  private static final int MAX_KEY_SIZE =
      GlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final AlwaysLessKey ALWAYS_LESS_KEY = new AlwaysLessKey();
  private static final AlwaysGreaterKey ALWAYS_GREATER_KEY = new AlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH =
      GlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final int ENTRY_POINT_INDEX = 0;
  private static final long ROOT_INDEX = 1;
  private final Comparator<? super K> comparator = DefaultComparator.INSTANCE;

  private final String nullFileExtension;
  private long fileId;
  private long nullBucketFileId = -1;
  private int keySize;
  private BinarySerializer<K> keySerializer;
  private PropertyType[] keyTypes;
  private Encryption encryption;

  public CellBTreeSingleValueV1(
      final String name,
      final String dataFileExtension,
      final String nullFileExtension,
      final AbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(
      AtomicOperation atomicOperation,
      final BinarySerializer<K> keySerializer,
      final PropertyType[] keyTypes,
      final int keySize,
      final Encryption encryption) {
    assert keySerializer != null;
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {

            this.keySize = keySize;
            if (keyTypes != null) {
              this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
            } else {
              this.keyTypes = null;
            }

            this.encryption = encryption;
            this.keySerializer = keySerializer;

            fileId = addFile(atomicOperation, getFullName());
            nullBucketFileId = addFile(atomicOperation, getName() + nullFileExtension);

            try (final CacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId)) {
              final CellBTreeSingleValueEntryPointV1 entryPoint =
                  new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
              entryPoint.init();
            }

            try (final CacheEntry rootCacheEntry = addPage(atomicOperation, fileId)) {
              @SuppressWarnings("unused") final CellBTreeBucketSingleValueV1<K> rootBucket =
                  new CellBTreeBucketSingleValueV1<>(rootCacheEntry);
              rootBucket.init(true);
            }

            try (final CacheEntry nullCacheEntry = addPage(atomicOperation, nullBucketFileId)) {
              @SuppressWarnings("unused") final CellBTreeNullBucketSingleValueV1 nullBucket =
                  new CellBTreeNullBucketSingleValueV1(nullCacheEntry);
              nullBucket.init();
            }

          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public RID get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return null;
          }

          final long pageIndex = bucketSearchResult.pageIndex;

          try (final CacheEntry keyBucketCacheEntry =
              loadPageForRead(atomicOperation, fileId, pageIndex)) {
            final CellBTreeBucketSingleValueV1<K> keyBucket =
                new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
            return keyBucket.getValue(bucketSearchResult.itemIndex, encryption, keySerializer);
          }
        } else {
          try (final CacheEntry nullBucketCacheEntry =
              loadPageForRead(atomicOperation, nullBucketFileId, 0)) {
            final CellBTreeNullBucketSingleValueV1 nullBucket =
                new CellBTreeNullBucketSingleValueV1(nullBucketCacheEntry);
            return nullBucket.getValue();
          }
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception(
              "Error during retrieving  of sbtree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void put(AtomicOperation atomicOperation, final K key, final RID value) {
    update(atomicOperation, key, value, null);
  }

  @Override
  public boolean validatedPut(
      AtomicOperation atomicOperation,
      final K key,
      final RID value,
      final IndexEngineValidator<K, RID> validator) {
    return update(atomicOperation, key, value, validator);
  }

  private boolean update(
      final AtomicOperation atomicOperation,
      K k,
      RID rid,
      final IndexEngineValidator<K, RID> validator) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            K key = k;
            RID value = rid;
            if (key != null) {

              key = keySerializer.preprocess(key, (Object[]) keyTypes);

              if (keySize > MAX_KEY_SIZE) {
                throw new TooBigIndexKeyException(
                    "Key size is more than allowed, operation was canceled. Current key size "
                        + keySize
                        + ", allowed  "
                        + MAX_KEY_SIZE,
                    getName());
              }

              UpdateBucketSearchResult bucketSearchResult =
                  findBucketForUpdate(key, atomicOperation);

              CacheEntry keyBucketCacheEntry =
                  loadPageForWrite(
                      atomicOperation, fileId, bucketSearchResult.getLastPathItem(), true);
              CellBTreeBucketSingleValueV1<K> keyBucket =
                  new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);

              final byte[] oldRawValue;
              if (bucketSearchResult.itemIndex > -1) {
                oldRawValue =
                    keyBucket.getRawValue(bucketSearchResult.itemIndex, encryption, keySerializer);
              } else {
                oldRawValue = null;
              }
              final RID oldValue;
              if (oldRawValue == null) {
                oldValue = null;
              } else {
                final int clusterId = ShortSerializer.INSTANCE.deserializeNative(oldRawValue, 0);
                final long clusterPosition =
                    LongSerializer.INSTANCE.deserializeNative(
                        oldRawValue, ShortSerializer.SHORT_SIZE);
                oldValue = new RecordId(clusterId, clusterPosition);
              }

              if (validator != null) {
                boolean failure = true; // assuming validation throws by default
                boolean ignored = false;

                try {

                  final Object result = validator.validate(key, oldValue, value);
                  if (result == IndexEngineValidator.IGNORE) {
                    ignored = true;
                    failure = false;
                    return false;
                  }

                  value = (RID) result;
                  failure = false;
                } finally {
                  if (failure || ignored) {
                    keyBucketCacheEntry.close();
                  }
                }
              }

              final byte[] serializedValue =
                  new byte[ShortSerializer.SHORT_SIZE + LongSerializer.LONG_SIZE];
              ShortSerializer.INSTANCE.serializeNative(
                  (short) value.getClusterId(), serializedValue, 0);
              LongSerializer.INSTANCE.serializeNative(
                  value.getClusterPosition(), serializedValue, ShortSerializer.SHORT_SIZE);

              final byte[] rawKey = serializeKey(key);
              int insertionIndex;
              final int sizeDiff;
              if (bucketSearchResult.itemIndex >= 0) {
                assert oldRawValue != null;

                if (oldRawValue.length == serializedValue.length) {
                  keyBucket.updateValue(
                      bucketSearchResult.itemIndex, serializedValue, rawKey.length);
                  keyBucketCacheEntry.close();
                  return true;
                } else {
                  keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, rawKey, oldRawValue);
                  insertionIndex = bucketSearchResult.itemIndex;
                  sizeDiff = 0;
                }
              } else {
                insertionIndex = -bucketSearchResult.itemIndex - 1;
                sizeDiff = 1;
              }

              while (!keyBucket.addLeafEntry(insertionIndex, rawKey, serializedValue)) {
                bucketSearchResult =
                    splitBucket(
                        keyBucket,
                        keyBucketCacheEntry,
                        bucketSearchResult.path,
                        bucketSearchResult.insertionIndexes,
                        insertionIndex,
                        atomicOperation);

                insertionIndex = bucketSearchResult.itemIndex;

                final long pageIndex = bucketSearchResult.getLastPathItem();

                if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
                  keyBucketCacheEntry.close();

                  keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
                }

                //noinspection ObjectAllocationInLoop
                keyBucket = new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
              }

              keyBucketCacheEntry.close();

              if (sizeDiff != 0) {
                updateSize(sizeDiff, atomicOperation);
              }
            } else {

              int sizeDiff = 0;
              final RID oldValue;
              try (final CacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, nullBucketFileId, 0, true)) {
                final CellBTreeNullBucketSingleValueV1 nullBucket =
                    new CellBTreeNullBucketSingleValueV1(cacheEntry);
                oldValue = nullBucket.getValue();

                if (validator != null) {
                  final Object result = validator.validate(null, oldValue, value);
                  if (result == IndexEngineValidator.IGNORE) {
                    return false;
                  }
                }

                if (oldValue != null) {
                  sizeDiff = -1;
                }

                nullBucket.setValue(value);
              }

              sizeDiff++;
              updateSize(sizeDiff, atomicOperation);
            }
            return true;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void close() {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, true, writeCache);
      readCache.closeFile(nullBucketFileId, true, writeCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete(AtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            deleteFile(atomicOperation, fileId);
            deleteFile(atomicOperation, nullBucketFileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void load(
      final String name,
      final int keySize,
      final PropertyType[] keyTypes,
      final BinarySerializer<K> keySerializer,
      final Encryption encryption) {
    acquireExclusiveLock();
    try {
      final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
      nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);

      this.keySize = keySize;
      this.keyTypes = keyTypes;
      this.keySerializer = keySerializer;
      this.encryption = encryption;
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception("Exception during loading of sbtree " + name, this),
          e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        try (final CacheEntry entryPointCacheEntry =
            loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX)) {
          final CellBTreeSingleValueEntryPointV1 entryPoint =
              new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
          return entryPoint.getTreeSize();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception(
              "Error during retrieving of size of index " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public RID remove(AtomicOperation atomicOperation, K k) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final RID removedValue;

            K key = k;
            if (key != null) {
              key = keySerializer.preprocess(key, (Object[]) keyTypes);

              final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
              if (bucketSearchResult.itemIndex < 0) {
                return null;
              }

              final byte[] rawKey = serializeKey(key);

              try (final CacheEntry keyBucketCacheEntry =
                  loadPageForWrite(atomicOperation, fileId, bucketSearchResult.pageIndex, true)) {
                final CellBTreeBucketSingleValueV1<K> keyBucket =
                    new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
                final byte[] rawRemovedValue =
                    keyBucket.getRawValue(bucketSearchResult.itemIndex, encryption, keySerializer);

                final int clusterId =
                    ShortSerializer.INSTANCE.deserializeNative(rawRemovedValue, 0);
                final long clusterPosition =
                    LongSerializer.INSTANCE.deserializeNative(
                        rawRemovedValue, ShortSerializer.SHORT_SIZE);

                removedValue = new RecordId(clusterId, clusterPosition);

                keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, rawKey, rawRemovedValue);
                updateSize(-1, atomicOperation);
              }
            } else {
              if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
                return null;
              }

              removedValue = removeNullBucket(atomicOperation);
            }
            return removedValue;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private RID removeNullBucket(final AtomicOperation atomicOperation) throws IOException {
    RID removedValue;
    try (final CacheEntry nullCacheEntry =
        loadPageForWrite(atomicOperation, nullBucketFileId, 0, true)) {
      final CellBTreeNullBucketSingleValueV1 nullBucket =
          new CellBTreeNullBucketSingleValueV1(nullCacheEntry);
      removedValue = nullBucket.getValue();

      if (removedValue != null) {
        nullBucket.removeValue();
      }
    }

    if (removedValue != null) {
      updateSize(-1, atomicOperation);
    }

    return removedValue;
  }

  @Override
  public Stream<RawPair<K, RID>> iterateEntriesMinor(
      final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (!ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMinorDesc(key, inclusive), false);
        }

        return StreamSupport.stream(iterateEntriesMinorAsc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Stream<RawPair<K, RID>> iterateEntriesMajor(
      final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMajorAsc(key, inclusive), false);
        }

        return StreamSupport.stream(iterateEntriesMajorDesc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        try (final CacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, searchResult.pageIndex)) {
          final CellBTreeBucketSingleValueV1<K> bucket =
              new CellBTreeBucketSingleValueV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception(
              "Error during finding first key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = lastItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        try (final CacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, searchResult.pageIndex)) {
          final CellBTreeBucketSingleValueV1<K> bucket =
              new CellBTreeBucketSingleValueV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception(
              "Error during finding last key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Stream<K> keyStream() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return StreamSupport.stream(Spliterators.emptySpliterator(), false);
        }

        return StreamSupport.stream(
                new CellBTreeSpliteratorForward(null, null, false, false), false)
            .map((entry) -> entry.first);
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new CellBTreeSingleValueV1Exception(
              "Error during finding first key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<RawPair<K, RID>> allEntries() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return StreamSupport.stream(
            new CellBTreeSpliteratorForward(null, null, false, false), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Stream<RawPair<K, RID>> iterateEntriesBetween(
      final K keyFrom,
      final boolean fromInclusive,
      final K keyTo,
      final boolean toInclusive,
      final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(
              iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        } else {
          return StreamSupport.stream(
              iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * SB-tree.
   */
  @Override
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  private void updateSize(final long diffSize, final AtomicOperation atomicOperation)
      throws IOException {
    try (final CacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final CellBTreeSingleValueEntryPointV1 entryPoint =
          new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    }
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesMinorDesc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new CellBTreeSpliteratorBackward(null, key, false, inclusive);
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesMinorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    return new CellBTreeSpliteratorForward(null, key, false, inclusive);
  }

  private K enhanceCompositeKeyMinorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMinorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesMajorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new CellBTreeSpliteratorForward(key, null, inclusive, false);
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesMajorDesc(K key, final boolean inclusive) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMajorDesc(key, inclusive);

      return new CellBTreeSpliteratorBackward(key, null, inclusive, false);

    } finally {
      releaseSharedLock();
    }
  }

  private K enhanceCompositeKeyMajorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMajorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private BucketSearchResult firstItem(final AtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    CacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);
    int itemIndex = 0;
    try {
      CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex < bucket.size()) {
              bucketIndex = bucket.getLeft(itemIndex);
            } else {
              bucketIndex = bucket.getRight(itemIndex - 1);
            }

            itemIndex = 0;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(0, bucketIndex);
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

        //noinspection ObjectAllocationInLoop
        bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
      }
    } finally {
      cacheEntry.close();
    }
  }

  private BucketSearchResult lastItem(final AtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    CacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

    CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              bucketIndex = bucket.getRight(itemIndex);
            } else {
              bucketIndex = bucket.getLeft(0);
            }

            itemIndex = CellBTreeBucketSingleValueV1.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(bucket.size() - 1, bucketIndex);
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

        bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
        if (itemIndex == CellBTreeBucketSingleValueV1.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      cacheEntry.close();
    }
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesBetweenAscOrder(
      K keyFrom, final boolean fromInclusive, K keyTo, final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new CellBTreeSpliteratorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private Spliterator<RawPair<K, RID>> iterateEntriesBetweenDescOrder(
      K keyFrom, final boolean fromInclusive, K keyTo, final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new CellBTreeSpliteratorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private K enhanceToCompositeKeyBetweenAsc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenAsc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private K enhanceToCompositeKeyBetweenDesc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenDesc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private UpdateBucketSearchResult splitBucket(
      final CellBTreeBucketSingleValueV1<K> bucketToSplit,
      final CacheEntry entryToSplit,
      final LongList path,
      final IntList itemPointers,
      final int keyIndex,
      final AtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit = bucketSize >>> 1;
    final K separationKey = bucketToSplit.getKey(indexToSplit, encryption, keySerializer);
    final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    for (int i = startRightIndex; i < bucketSize; i++) {
      rightEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer));
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(
          path,
          itemPointers,
          keyIndex,
          entryToSplit.getPageIndex(),
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    } else {
      return splitRootBucket(
          keyIndex,
          entryToSplit,
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    }
  }

  private UpdateBucketSearchResult splitNonRootBucket(
      final LongList path,
      final IntList itemPointers,
      final int keyIndex,
      final long pageIndex,
      final CellBTreeBucketSingleValueV1<K> bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final K separationKey,
      final List<byte[]> rightEntries,
      final AtomicOperation atomicOperation)
      throws IOException {

    final CacheEntry rightBucketEntry;
    try (final CacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final CellBTreeSingleValueEntryPointV1 entryPoint =
          new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      int pageSize = entryPoint.getPagesSize();

      if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
        pageSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false);
        entryPoint.setPagesSize(pageSize);
      } else {
        assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

        rightBucketEntry = addPage(atomicOperation, fileId);
        entryPoint.setPagesSize(rightBucketEntry.getPageIndex());
      }
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newRightBucket =
          new CellBTreeBucketSingleValueV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);

      newRightBucket.addAll(rightEntries, encryption != null, keySerializer);

      bucketToSplit.shrink(indexToSplit, encryption != null, keySerializer);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {

          try (final CacheEntry rightSiblingBucketEntry =
              loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true)) {
            final CellBTreeBucketSingleValueV1<K> rightSiblingBucket =
                new CellBTreeBucketSingleValueV1<>(rightSiblingBucketEntry);
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          }
        }
      }

      long parentIndex = path.getLong(path.size() - 2);
      CacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
      try {
        CellBTreeBucketSingleValueV1<K> parentBucket =
            new CellBTreeBucketSingleValueV1<>(parentCacheEntry);
        int insertionIndex = itemPointers.getInt(itemPointers.size() - 2);
        while (!parentBucket.addNonLeafEntry(
            insertionIndex,
            (int) pageIndex,
            rightBucketEntry.getPageIndex(),
            serializeKey(separationKey),
            true)) {
          final UpdateBucketSearchResult bucketSearchResult =
              splitBucket(
                  parentBucket,
                  parentCacheEntry,
                  path.subList(0, path.size() - 1),
                  itemPointers.subList(0, itemPointers.size() - 1),
                  insertionIndex,
                  atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          insertionIndex = bucketSearchResult.itemIndex;

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            parentCacheEntry.close();

            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
          }

          //noinspection ObjectAllocationInLoop
          parentBucket = new CellBTreeBucketSingleValueV1<>(parentCacheEntry);
        }

      } finally {
        parentCacheEntry.close();
      }

    } finally {
      rightBucketEntry.close();
    }

    final LongArrayList resultPath = new LongArrayList(path.subList(0, path.size() - 1));
    final IntArrayList resultItemPointers =
        new IntArrayList(itemPointers.subList(0, itemPointers.size() - 1));

    if (keyIndex <= indexToSplit) {
      resultPath.add(pageIndex);
      resultItemPointers.add(keyIndex);

      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
    }

    final int parentIndex = resultItemPointers.size() - 1;
    resultItemPointers.set(parentIndex, resultItemPointers.getInt(parentIndex) + 1);
    resultPath.add(rightBucketEntry.getPageIndex());

    if (splitLeaf) {
      resultItemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
    }

    resultItemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(
        resultItemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private byte[] serializeKey(K separationKey) {
    final byte[] serializedKey =
        keySerializer.serializeNativeAsWhole(separationKey, (Object[]) keyTypes);
    final byte[] rawKey;
    if (encryption == null) {
      rawKey = serializedKey;
    } else {
      final byte[] encryptedKey = encryption.encrypt(serializedKey);
      rawKey = new byte[encryptedKey.length + IntegerSerializer.INT_SIZE];
      IntegerSerializer.INSTANCE.serializeNative(encryptedKey.length, rawKey, 0);
      System.arraycopy(encryptedKey, 0, rawKey, IntegerSerializer.INT_SIZE, encryptedKey.length);
    }
    return rawKey;
  }

  private UpdateBucketSearchResult splitRootBucket(
      final int keyIndex,
      final CacheEntry bucketEntry,
      CellBTreeBucketSingleValueV1<K> bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final K separationKey,
      final List<byte[]> rightEntries,
      final AtomicOperation atomicOperation)
      throws IOException {
    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer));
    }

    final CacheEntry leftBucketEntry;
    final CacheEntry rightBucketEntry;

    try (final CacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final CellBTreeSingleValueEntryPointV1 entryPoint =
          new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      int pagesSize = entryPoint.getPagesSize();

      final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

      if (pagesSize < filledUpTo - 1) {
        pagesSize++;
        leftBucketEntry = loadPageForWrite(atomicOperation, fileId, pagesSize, false);
      } else {
        assert pagesSize == filledUpTo - 1;
        leftBucketEntry = addPage(atomicOperation, fileId);
        pagesSize = leftBucketEntry.getPageIndex();
      }

      if (pagesSize < filledUpTo) {
        pagesSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pagesSize, false);
      } else {
        assert pagesSize == filledUpTo;
        rightBucketEntry = addPage(atomicOperation, fileId);
        pagesSize = rightBucketEntry.getPageIndex();
      }

      entryPoint.setPagesSize(pagesSize);
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newLeftBucket =
          new CellBTreeBucketSingleValueV1<>(leftBucketEntry);
      newLeftBucket.init(splitLeaf);

      newLeftBucket.addAll(leftEntries, encryption != null, keySerializer);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      leftBucketEntry.close();
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newRightBucket =
          new CellBTreeBucketSingleValueV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);

      newRightBucket.addAll(rightEntries, encryption != null, keySerializer);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      rightBucketEntry.close();
    }

    bucketToSplit = new CellBTreeBucketSingleValueV1<>(bucketEntry);
    bucketToSplit.shrink(0, encryption != null, keySerializer);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }

    bucketToSplit.addNonLeafEntry(
        0,
        leftBucketEntry.getPageIndex(),
        rightBucketEntry.getPageIndex(),
        serializeKey(separationKey),
        true);

    final LongArrayList resultPath = new LongArrayList(8);
    resultPath.add(ROOT_INDEX);

    final IntArrayList itemPointers = new IntArrayList(8);

    if (keyIndex <= indexToSplit) {
      itemPointers.add(-1);
      itemPointers.add(keyIndex);

      resultPath.add(leftBucketEntry.getPageIndex());
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
    }

    resultPath.add(rightBucketEntry.getPageIndex());
    itemPointers.add(0);

    if (splitLeaf) {
      itemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
    }

    itemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private BucketSearchResult findBucket(final K key, final AtomicOperation atomicOperation)
      throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new CellBTreeSingleValueV1Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in"
                + " corrupted state. You should rebuild index related to given query.",
            this);
      }

      try (final CacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        @SuppressWarnings("ObjectAllocationInLoop") final CellBTreeBucketSingleValueV1<K> keyBucket =
            new CellBTreeBucketSingleValueV1<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer, encryption);

        if (keyBucket.isLeaf()) {
          return new BucketSearchResult(index, pageIndex);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }
        }
      }
    }
  }

  private UpdateBucketSearchResult findBucketForUpdate(
      final K key, final AtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    final LongArrayList path = new LongArrayList(8);
    final IntArrayList itemIndexes = new IntArrayList(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new CellBTreeSingleValueV1Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in"
                + " corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);
      try (final CacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        @SuppressWarnings("ObjectAllocationInLoop") final CellBTreeBucketSingleValueV1<K> keyBucket =
            new CellBTreeBucketSingleValueV1<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer, encryption);

        if (keyBucket.isLeaf()) {
          itemIndexes.add(index);
          return new UpdateBucketSearchResult(itemIndexes, path, index);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
          itemIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }

          itemIndexes.add(insertionIndex);
        }
      }
    }
  }

  private K enhanceCompositeKey(final K key, final PartialSearchMode partialSearchMode) {
    if (!(key instanceof CompositeKey compositeKey)) {
      return key;
    }

    if (!(keySize == 1
        || compositeKey.getKeys().size() == keySize
        || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final CompositeKey fullKey = new CompositeKey(compositeKey);
      final int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY)) {
        keyItem = ALWAYS_GREATER_KEY;
      } else {
        keyItem = ALWAYS_LESS_KEY;
      }

      for (int i = 0; i < itemsToAdd; i++) {
        fullKey.addKey(keyItem);
      }

      //noinspection unchecked
      return (K) fullKey;
    }

    return key;
  }

  private RawPair<K, RID> convertToMapEntry(
      final CellBTreeBucketSingleValueV1.SBTreeEntry<K> treeEntry) {
    final K key = treeEntry.key;
    final RID value = treeEntry.value;

    return new RawPair<>(key, value);
  }

  /**
   * Indicates search behavior in case of {@link CompositeKey} keys that have less amount of
   * internal keys are used, whether lowest or highest partially matched key should be used.
   */
  private enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE,
    /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  private static final class BucketSearchResult {

    private final int itemIndex;
    private final long pageIndex;

    private BucketSearchResult(final int itemIndex, final long pageIndex) {
      this.itemIndex = itemIndex;
      this.pageIndex = pageIndex;
    }
  }

  private static final class UpdateBucketSearchResult {

    private final IntList insertionIndexes;
    private final LongArrayList path;
    private final int itemIndex;

    private UpdateBucketSearchResult(
        final IntList insertionIndexes, final LongArrayList path, final int itemIndex) {
      this.insertionIndexes = insertionIndexes;
      this.path = path;
      this.itemIndex = itemIndex;
    }

    private long getLastPathItem() {
      return path.getLong(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {

    private final long pageIndex;
    private final int itemIndex;

    private PagePathItemUnit(final long pageIndex, final int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  private final class CellBTreeSpliteratorForward implements Spliterator<RawPair<K, RID>> {

    private K fromKey;
    private final K toKey;
    private boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private final List<RawPair<K, RID>> dataCache = new ArrayList<>();
    private Iterator<RawPair<K, RID>> dataCacheIterator = Collections.emptyIterator();

    private CellBTreeSpliteratorForward(
        final K fromKey,
        final K toKey,
        final boolean fromKeyInclusive,
        final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (fromKey == null) {
        this.fromKeyInclusive = true;
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super RawPair<K, RID>> action) {
      if (dataCacheIterator == null) {
        return false;
      }

      if (dataCacheIterator.hasNext()) {
        final RawPair<K, RID> entry = dataCacheIterator.next();

        fromKey = entry.first;
        fromKeyInclusive = false;

        action.accept(entry);
        return true;
      }

      dataCache.clear();

      final int prefetchSize = GlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

      atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV1.this);
      try {
        acquireSharedLock();
        try {
          final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (fromKey != null) {
            bucketSearchResult = findBucket(fromKey, atomicOperation);
          } else {
            bucketSearchResult = firstItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return false;
          }

          long pageIndex = bucketSearchResult.pageIndex;
          int itemIndex;

          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex =
                fromKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 1;
          }

          mainCycle:
          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            try (final CacheEntry cacheEntry =
                loadPageForRead(atomicOperation, fileId, pageIndex)) {
              @SuppressWarnings("ObjectAllocationInLoop") final CellBTreeBucketSingleValueV1<K> bucket =
                  new CellBTreeBucketSingleValueV1<>(cacheEntry);

              final int bucketSize = bucket.size();
              if (itemIndex >= bucketSize) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              while (itemIndex < bucketSize && dataCache.size() < prefetchSize) {
                @SuppressWarnings("ObjectAllocationInLoop") final RawPair<K, RID> entry =
                    convertToMapEntry(bucket.getEntry(itemIndex, encryption, keySerializer));
                itemIndex++;

                if (fromKey != null) {
                  if (fromKeyInclusive) {
                    if (comparator.compare(entry.first, fromKey) < 0) {
                      continue;
                    }
                  } else if (comparator.compare(entry.first, fromKey) <= 0) {
                    continue;
                  }
                }

                if (toKey != null) {
                  if (toKeyInclusive) {
                    if (comparator.compare(entry.first, toKey) > 0) {
                      break mainCycle;
                    }
                  } else if (comparator.compare(entry.first, toKey) >= 0) {
                    break mainCycle;
                  }
                }

                dataCache.add(entry);
              }
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw BaseException.wrapException(
            new CellBTreeSingleValueV1Exception(
                "Error during entity iteration", CellBTreeSingleValueV1.this),
            e);
      } finally {
        atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return false;
      }

      dataCacheIterator = dataCache.iterator();

      final RawPair<K, RID> entry = dataCacheIterator.next();

      fromKey = entry.first;
      fromKeyInclusive = false;

      action.accept(entry);
      return true;
    }

    @Override
    public Spliterator<RawPair<K, RID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return SORTED | NONNULL | ORDERED;
    }

    @Override
    public Comparator<? super RawPair<K, RID>> getComparator() {
      return (pairOne, pairTwo) -> comparator.compare(pairOne.first, pairTwo.first);
    }
  }

  private final class CellBTreeSpliteratorBackward implements Spliterator<RawPair<K, RID>> {

    private final K fromKey;
    private K toKey;
    private final boolean fromKeyInclusive;
    private boolean toKeyInclusive;

    private final List<RawPair<K, RID>> dataCache = new ArrayList<>();
    private Iterator<RawPair<K, RID>> dataCacheIterator = Collections.emptyIterator();

    private CellBTreeSpliteratorBackward(
        final K fromKey,
        final K toKey,
        final boolean fromKeyInclusive,
        final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (toKey == null) {
        this.toKeyInclusive = true;
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super RawPair<K, RID>> action) {
      if (dataCacheIterator == null) {
        return false;
      }

      if (dataCacheIterator.hasNext()) {
        final RawPair<K, RID> entry = dataCacheIterator.next();
        toKey = entry.first;

        toKeyInclusive = false;
        action.accept(entry);
        return true;
      }

      dataCache.clear();

      final int prefetchSize = GlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

      atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV1.this);
      try {
        acquireSharedLock();
        try {
          final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (toKey != null) {
            bucketSearchResult = findBucket(toKey, atomicOperation);
          } else {
            bucketSearchResult = lastItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return false;
          }

          long pageIndex = bucketSearchResult.pageIndex;

          int itemIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex =
                toKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 2;
          }

          mainCycle:
          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            try (final CacheEntry cacheEntry =
                loadPageForRead(atomicOperation, fileId, pageIndex)) {
              @SuppressWarnings("ObjectAllocationInLoop") final CellBTreeBucketSingleValueV1<K> bucket =
                  new CellBTreeBucketSingleValueV1<>(cacheEntry);

              if (itemIndex >= bucket.size()) {
                itemIndex = bucket.size() - 1;
              }

              if (itemIndex < 0) {
                pageIndex = bucket.getLeftSibling();
                itemIndex = Integer.MAX_VALUE;
                continue;
              }

              while (itemIndex >= 0 && dataCache.size() < prefetchSize) {
                @SuppressWarnings("ObjectAllocationInLoop") final RawPair<K, RID> entry =
                    convertToMapEntry(bucket.getEntry(itemIndex, encryption, keySerializer));
                itemIndex--;

                if (toKey != null) {
                  if (toKeyInclusive) {
                    if (comparator.compare(entry.first, toKey) > 0) {
                      continue;
                    }
                  } else if (comparator.compare(entry.first, toKey) >= 0) {
                    continue;
                  }
                }

                if (fromKey != null) {
                  if (fromKeyInclusive) {
                    if (comparator.compare(entry.first, fromKey) < 0) {
                      break mainCycle;
                    }
                  } else if (comparator.compare(entry.first, fromKey) <= 0) {
                    break mainCycle;
                  }
                }

                dataCache.add(entry);
              }
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw BaseException.wrapException(
            new CellBTreeSingleValueV1Exception(
                "Error during entity iteration", CellBTreeSingleValueV1.this),
            e);
      } finally {
        atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return false;
      }

      dataCacheIterator = dataCache.iterator();

      final RawPair<K, RID> entry = dataCacheIterator.next();

      toKey = entry.first;
      toKeyInclusive = false;

      action.accept(entry);
      return true;
    }

    @Override
    public Spliterator<RawPair<K, RID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return SORTED | NONNULL | ORDERED;
    }

    @Override
    public Comparator<? super RawPair<K, RID>> getComparator() {
      return (pairOne, pairTwo) -> -comparator.compare(pairOne.first, pairTwo.first);
    }
  }
}
