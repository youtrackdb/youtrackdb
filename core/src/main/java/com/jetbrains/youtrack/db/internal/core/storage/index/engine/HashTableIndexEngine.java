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
package com.jetbrains.youtrack.db.internal.core.storage.index.engine;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.IndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValuesTransformer;
import com.jetbrains.youtrack.db.internal.core.iterator.EmptyIterator;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.MurmurHash3HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashTable;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.SHA256HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2.LocalHashTableV2;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v3.LocalHashTableV3;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.VersionPositionMap;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.VersionPositionMapV0;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @since 15.07.13
 */
public final class HashTableIndexEngine implements IndexEngine {

  public static final int VERSION = 3;

  public static final String METADATA_FILE_EXTENSION = ".him";
  public static final String TREE_FILE_EXTENSION = ".hit";
  public static final String BUCKET_FILE_EXTENSION = ".hib";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final HashTable<Object, Object> hashTable;
  private final AtomicLong bonsayFileId = new AtomicLong(0);

  private final String name;

  private final int id;

  private final VersionPositionMap versionPositionMap;

  private final AbstractPaginatedStorage storage;

  public HashTableIndexEngine(
      String name, int id, AbstractPaginatedStorage storage, int version) {
    this.storage = storage;
    this.id = id;
    if (version < 2) {
      throw new IllegalStateException("Unsupported version of hash index");
    } else if (version == 2) {
      hashTable =
          new LocalHashTableV2<>(
              name,
              METADATA_FILE_EXTENSION,
              TREE_FILE_EXTENSION,
              BUCKET_FILE_EXTENSION,
              NULL_BUCKET_FILE_EXTENSION,
              storage);
    } else if (version == 3) {
      hashTable =
          new LocalHashTableV3<>(
              name,
              METADATA_FILE_EXTENSION,
              TREE_FILE_EXTENSION,
              BUCKET_FILE_EXTENSION,
              NULL_BUCKET_FILE_EXTENSION,
              storage);
    } else {
      throw new IllegalStateException("Invalid value of the index version , version = " + version);
    }
    versionPositionMap =
        new VersionPositionMapV0(
            storage, name, name + TREE_FILE_EXTENSION, VersionPositionMap.DEF_EXTENSION);
    this.name = name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(IndexMetadata metadata) {
  }

  @Override
  public String getName() {
    return name;
  }

  public void create(AtomicOperation atomicOperation, IndexEngineData data) throws IOException {

    BinarySerializer valueSerializer =
        storage.resolveObjectSerializer(data.getValueSerializerId());
    BinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());

    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());
    final HashFunction<Object> hashFunction;

    if (encryption != null) {
      hashFunction = new SHA256HashFunction<>(keySerializer);
    } else {
      hashFunction = new MurmurHash3HashFunction<>(keySerializer);
    }

    hashTable.create(
        atomicOperation,
        keySerializer,
        valueSerializer,
        data.getKeyTypes(),
        encryption,
        hashFunction,
        data.isNullValuesSupport());
    versionPositionMap.create(atomicOperation);
  }

  @Override
  public void flush() {
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return name;
  }

  @Override
  public void updateUniqueIndexVersion(final Object key) {
    final int keyHash = versionPositionMap.getKeyHash(key);
    versionPositionMap.updateVersion(keyHash);
  }

  @Override
  public int getUniqueIndexVersion(final Object key) {
    final int keyHash = versionPositionMap.getKeyHash(key);
    return versionPositionMap.getVersion(keyHash);
  }

  @Override
  public void delete(AtomicOperation atomicOperation) throws IOException {
    doClearTable(atomicOperation);

    hashTable.delete(atomicOperation);
    versionPositionMap.delete(atomicOperation);
  }

  private void doClearTable(AtomicOperation atomicOperation) throws IOException {
    final HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();

    if (firstEntry != null) {
      HashTable.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);
      while (entries.length > 0) {
        for (final HashTable.Entry<Object, Object> entry : entries) {
          hashTable.remove(atomicOperation, entry.key);
        }

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }
    }

    if (hashTable.isNullKeyIsSupported()) {
      hashTable.remove(atomicOperation, null);
    }
  }

  @Override
  public void load(IndexEngineData data) {
    BinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());
    BinarySerializer valueSerializer =
        storage.resolveObjectSerializer(data.getValueSerializerId());

    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    final HashFunction<Object> hashFunction;

    if (encryption != null) {
      //noinspection unchecked
      hashFunction = new SHA256HashFunction<>(keySerializer);
    } else {
      //noinspection unchecked
      hashFunction = new MurmurHash3HashFunction<>(keySerializer);
    }
    //noinspection unchecked
    hashTable.load(
        data.getName(),
        data.getKeyTypes(),
        data.isNullValuesSupport(),
        encryption,
        hashFunction,
        keySerializer,
        valueSerializer);

    try {
      versionPositionMap.open();
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during VPM load of index " + data.getName()), e);
    }
  }

  @Override
  public boolean remove(AtomicOperation atomicOperation, Object key) throws IOException {
    return hashTable.remove(atomicOperation, key) != null;
  }

  @Override
  public void clear(AtomicOperation atomicOperation) throws IOException {
    doClearTable(atomicOperation);
  }

  @Override
  public void close() {
    hashTable.close();
  }

  @Override
  public Object get(DatabaseSessionInternal session, Object key) {
    return hashTable.get(key);
  }

  @Override
  public void put(DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      Object value) throws IOException {
    hashTable.put(atomicOperation, key, value);
  }

  @Override
  public void update(DatabaseSessionInternal session, AtomicOperation atomicOperation,
      Object key,
      IndexKeyUpdater<Object> updater)
      throws IOException {
    Object value = get(session, key);
    IndexUpdateAction<Object> updated = updater.update(value, bonsayFileId);
    if (updated.isChange()) {
      put(session, atomicOperation, key, updated.getValue());
    } else if (updated.isRemove()) {
      remove(atomicOperation, key);
    } else //noinspection StatementWithEmptyBody
      if (updated.isNothing()) {
        // Do nothing
      }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(
      AtomicOperation atomicOperation,
      Object key,
      RID value,
      IndexEngineValidator<Object, RID> validator)
      throws IOException {
    return hashTable.validatedPut(atomicOperation, key, value, (IndexEngineValidator) validator);
  }

  @Override
  public long size(IndexEngineValuesTransformer transformer) {
    if (transformer == null) {
      return hashTable.size();
    } else {
      long counter = 0;

      if (hashTable.isNullKeyIsSupported()) {
        final Object nullValue = hashTable.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
      if (firstEntry == null) {
        return counter;
      }

      HashTable.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);

      while (entries.length > 0) {
        for (HashTable.Entry<Object, Object> entry : entries) {
          counter += transformer.transformFromValue(entry.value).size();
        }

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }

      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesBetween(
      DatabaseSessionInternal session, Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesMinor(
      Object toKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public Stream<RawPair<Object, RID>> stream(
      final IndexEngineValuesTransformer valuesTransformer) {
    return StreamSupport.stream(
        new Spliterator<RawPair<Object, RID>>() {
          private int nextEntriesIndex;
          private HashTable.Entry<Object, Object>[] entries;

          private Iterator<RID> currentIterator = new EmptyIterator<>();
          private Object currentKey;

          {
            HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
            if (firstEntry == null) {
              //noinspection unchecked
              entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              entries = hashTable.ceilingEntries(firstEntry.key);
            }

            if (entries.length == 0) {
              currentIterator = null;
            }
          }

          @Override
          public boolean tryAdvance(Consumer<? super RawPair<Object, RID>> action) {
            if (currentIterator == null) {
              return false;
            }

            if (currentIterator.hasNext()) {
              final Identifiable identifiable = currentIterator.next();
              action.accept(new RawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            while (currentIterator != null && !currentIterator.hasNext()) {
              if (entries.length == 0) {
                currentIterator = null;
                return false;
              }

              final HashTable.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

              currentKey = bucketEntry.key;

              Object value = bucketEntry.value;
              if (valuesTransformer != null) {
                currentIterator = valuesTransformer.transformFromValue(value).iterator();
              } else {
                currentIterator = Collections.singletonList((RID) value).iterator();
              }

              nextEntriesIndex++;

              if (nextEntriesIndex >= entries.length) {
                entries = hashTable.higherEntries(entries[entries.length - 1].key);

                nextEntriesIndex = 0;
              }
            }

            if (currentIterator != null) {
              final Identifiable identifiable = currentIterator.next();
              action.accept(new RawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            return false;
          }

          @Override
          public Spliterator<RawPair<Object, RID>> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return NONNULL;
          }
        },
        false);
  }

  @Override
  public Stream<RawPair<Object, RID>> descStream(
      final IndexEngineValuesTransformer valuesTransformer) {
    return StreamSupport.stream(
        new Spliterator<RawPair<Object, RID>>() {
          private int nextEntriesIndex;
          private HashTable.Entry<Object, Object>[] entries;

          private Iterator<RID> currentIterator = new EmptyIterator<>();
          private Object currentKey;

          {
            HashTable.Entry<Object, Object> lastEntry = hashTable.lastEntry();
            if (lastEntry == null) {
              //noinspection unchecked
              entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              entries = hashTable.floorEntries(lastEntry.key);
            }

            if (entries.length == 0) {
              currentIterator = null;
            }
          }

          @Override
          public boolean tryAdvance(Consumer<? super RawPair<Object, RID>> action) {
            if (currentIterator == null) {
              return false;
            }

            if (currentIterator.hasNext()) {
              final Identifiable identifiable = currentIterator.next();
              action.accept(new RawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            while (currentIterator != null && !currentIterator.hasNext()) {
              if (entries.length == 0) {
                currentIterator = null;
                return false;
              }

              final HashTable.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

              currentKey = bucketEntry.key;

              Object value = bucketEntry.value;
              if (valuesTransformer != null) {
                currentIterator = valuesTransformer.transformFromValue(value).iterator();
              } else {
                currentIterator = Collections.singletonList((RID) value).iterator();
              }

              nextEntriesIndex--;

              if (nextEntriesIndex < 0) {
                entries = hashTable.lowerEntries(entries[0].key);

                nextEntriesIndex = entries.length - 1;
              }
            }

            if (currentIterator != null) {
              final Identifiable identifiable = currentIterator.next();
              action.accept(new RawPair<>(currentKey, identifiable.getIdentity()));
              return true;
            }

            return false;
          }

          @Override
          public Spliterator<RawPair<Object, RID>> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return NONNULL;
          }
        },
        false);
  }

  @Override
  public Stream<Object> keyStream() {
    return StreamSupport.stream(
        new Spliterator<Object>() {
          private int nextEntriesIndex;
          private HashTable.Entry<Object, Object>[] entries;

          {
            HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
            if (firstEntry == null) {
              //noinspection unchecked
              entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              entries = hashTable.ceilingEntries(firstEntry.key);
            }
          }

          @Override
          public boolean tryAdvance(Consumer<? super Object> action) {
            if (entries.length == 0) {
              return false;
            }

            final HashTable.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];
            nextEntriesIndex++;
            if (nextEntriesIndex >= entries.length) {
              entries = hashTable.higherEntries(entries[entries.length - 1].key);

              nextEntriesIndex = 0;
            }

            action.accept(bucketEntry.key);
            return true;
          }

          @Override
          public Spliterator<Object> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return NONNULL;
          }
        },
        false);
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    hashTable.acquireAtomicExclusiveLock();
    return true;
  }

  public boolean hasRidBagTreesSupport() {
    return true;
  }
}
