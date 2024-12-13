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
package com.jetbrains.youtrack.db.internal.core.sharding.auto;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.IndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValuesTransformer;
import com.jetbrains.youtrack.db.internal.core.iterator.EmptyIterator;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashTable;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.MurmurHash3HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.SHA256HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2.LocalHashTableV2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Index engine implementation that relies on multiple hash indexes partitioned by key.
 */
public final class AutoShardingIndexEngine implements IndexEngine {

  public static final int VERSION = 1;
  private static final String SUBINDEX_METADATA_FILE_EXTENSION = ".asm";
  private static final String SUBINDEX_TREE_FILE_EXTENSION = ".ast";
  private static final String SUBINDEX_BUCKET_FILE_EXTENSION = ".asb";
  private static final String SUBINDEX_NULL_BUCKET_FILE_EXTENSION = ".asn";

  private final AbstractPaginatedStorage storage;
  private List<HashTable<Object, Object>> partitions;
  private AutoShardingStrategy strategy;
  private final String name;
  private int partitionSize;
  private final AtomicLong bonsayFileId = new AtomicLong(0);
  private final int id;

  AutoShardingIndexEngine(
      final String iName, int id, final AbstractPaginatedStorage iStorage, final int iVersion) {
    this.name = iName;
    this.id = id;
    this.storage = iStorage;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  public AutoShardingStrategy getStrategy() {
    return strategy;
  }

  public void create(AtomicOperation atomicOperation, IndexEngineData data) throws IOException {

    BinarySerializer valueSerializer =
        storage.resolveObjectSerializer(data.getValueSerializerId());
    BinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());

    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    this.strategy = new AutoShardingMurmurStrategy(keySerializer);

    final HashFunction<Object> hashFunction;

    if (encryption != null) {
      hashFunction = new SHA256HashFunction<>(keySerializer);
    } else {
      hashFunction = new MurmurHash3HashFunction<>(keySerializer);
    }
    Map<String, String> engineProperties = data.getEngineProperties();
    final String partitionsProperty = engineProperties.get("partitions");
    if (partitionsProperty != null) {
      try {
        this.partitionSize = Integer.parseInt(partitionsProperty);
      } catch (NumberFormatException e) {
        LogManager.instance()
            .error(
                this, "Invalid value of 'partitions' property : `" + partitionsProperty + "`", e);
      }
      engineProperties = new HashMap<>();
      engineProperties.put("partitions", String.valueOf(partitionSize));
    }

    init();

    try {
      for (HashTable<Object, Object> p : partitions) {
        p.create(
            atomicOperation,
            keySerializer,
            valueSerializer,
            data.getKeyTypes(),
            encryption,
            hashFunction,
            data.isNullValuesSupport());
      }
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during creation of index with name " + name), e);
    }
  }

  @Override
  public void load(IndexEngineData data) {
    BinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());
    BinarySerializer valueSerializer =
        storage.resolveObjectSerializer(data.getValueSerializerId());
    Map<String, String> engineProperties = data.getEngineProperties();

    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    this.strategy = new AutoShardingMurmurStrategy(keySerializer);

    if (storage != null) {
      final String partitionsAsString = engineProperties.get("partitions");
      if (partitionsAsString == null || partitionsAsString.isEmpty()) {
        throw new IndexException(
            "Cannot load autosharding index '"
                + data.getName()
                + "' because there is no metadata about the number of partitions");
      }

      partitionSize = Integer.parseInt(partitionsAsString);
      init();

      int i = 0;

      final HashFunction<Object> hashFunction;

      if (encryption != null) {
        //noinspection unchecked
        hashFunction = new SHA256HashFunction<>(keySerializer);
      } else {
        //noinspection unchecked
        hashFunction = new MurmurHash3HashFunction<>(keySerializer);
      }

      for (HashTable<Object, Object> p : partitions)
      //noinspection unchecked
      {
        p.load(
            data.getName() + "_" + (i++),
            data.getKeyTypes(),
            data.isNullValuesSupport(),
            encryption,
            hashFunction,
            keySerializer,
            valueSerializer);
      }
    }
  }

  @Override
  public void flush() {
    if (partitions != null) {
      for (HashTable<Object, Object> p : partitions) {
        p.flush();
      }
    }
  }

  @Override
  public void delete(AtomicOperation atomicOperation) {
    try {
      if (partitions != null) {
        doClearPartitions(atomicOperation);

        for (HashTable<Object, Object> p : partitions) {
          p.delete(atomicOperation);
        }
      }

    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during deletion of index with name " + name), e);
    }
  }

  private void doClearPartitions(final AtomicOperation atomicOperation) throws IOException {
    for (HashTable<Object, Object> p : partitions) {
      final HashTable.Entry<Object, Object> firstEntry = p.firstEntry();

      if (firstEntry != null) {
        HashTable.Entry<Object, Object>[] entries = p.ceilingEntries(firstEntry.key);
        while (entries.length > 0) {
          for (final HashTable.Entry<Object, Object> entry : entries) {
            p.remove(atomicOperation, entry.key);
          }

          entries = p.higherEntries(entries[entries.length - 1].key);
        }
      }

      if (p.isNullKeyIsSupported()) {
        p.remove(atomicOperation, null);
      }
    }
  }

  @Override
  public void init(IndexMetadata metadata) {
  }

  private void init() {
    if (partitions != null) {
      return;
    }

    partitions = new ArrayList<>(partitionSize);
    for (int i = 0; i < partitionSize; ++i) {
      partitions.add(
          new LocalHashTableV2<>(
              name + "_" + i,
              SUBINDEX_METADATA_FILE_EXTENSION,
              SUBINDEX_TREE_FILE_EXTENSION,
              SUBINDEX_BUCKET_FILE_EXTENSION,
              SUBINDEX_NULL_BUCKET_FILE_EXTENSION,
              storage));
    }
  }

  @Override
  public boolean remove(AtomicOperation atomicOperation, final Object key) {
    try {
      return getPartition(key).remove(atomicOperation, key) != null;
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException(
              "Error during deletion of key " + key + " of index with name " + name),
          e);
    }
  }

  @Override
  public void clear(AtomicOperation atomicOperation) {
    try {
      if (partitions != null) {
        doClearPartitions(atomicOperation);
      }
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during clear of index with name " + name), e);
    }
  }

  @Override
  public void close() {
    if (partitions != null) {
      for (HashTable<Object, Object> p : partitions) {
        p.close();
      }
    }
  }

  @Override
  public Object get(DatabaseSessionInternal session, final Object key) {
    return getPartition(key).get(key);
  }

  @Override
  public void put(DatabaseSessionInternal session, AtomicOperation atomicOperation,
      final Object key, final Object value) {
    try {
      getPartition(key).put(atomicOperation, key, value);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException(
              "Error during insertion of key " + key + " of index with name " + name),
          e);
    }
  }

  @Override
  public void update(
      DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      IndexKeyUpdater<Object> updater) {
    Object value = get(session, key);
    IndexUpdateAction<Object> updated = updater.update(value, bonsayFileId);
    if (updated.isChange()) {
      put(session, atomicOperation, key, updated.getValue());
    } else if (updated.isRemove()) {
      remove(atomicOperation, key);
    } else //noinspection StatementWithEmptyBody
      if (updated.isNothing()) {
        // Do Nothing
      }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(
      AtomicOperation atomicOperation,
      Object key,
      RID value,
      IndexEngineValidator<Object, RID> validator) {
    try {
      return getPartition(key)
          .validatedPut(atomicOperation, key, value, (IndexEngineValidator) validator);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException(
              "Error during insertion of key " + key + " of index with name " + name),
          e);
    }
  }

  @Override
  public long size(final IndexEngineValuesTransformer transformer) {
    long counter = 0;

    if (partitions != null) {
      for (HashTable<Object, Object> p : partitions) {
        if (transformer == null) {
          counter += p.size();
        } else {
          final HashTable.Entry<Object, Object> firstEntry = p.firstEntry();
          if (firstEntry == null) {
            continue;
          }

          HashTable.Entry<Object, Object>[] entries = p.ceilingEntries(firstEntry.key);

          while (entries.length > 0) {
            for (HashTable.Entry<Object, Object> entry : entries) {
              counter += transformer.transformFromValue(entry.value).size();
            }

            entries = p.higherEntries(entries[entries.length - 1].key);
          }
        }
      }
    }
    return counter;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public Stream<RawPair<Object, RID>> stream(
      final IndexEngineValuesTransformer valuesTransformer) {
    //noinspection resource
    return partitions.stream()
        .flatMap(
            (partition) ->
                StreamSupport.stream(
                    new HashTableSpliterator(valuesTransformer, partition), false));
  }

  @Override
  public Stream<RawPair<Object, RID>> descStream(
      final IndexEngineValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("descCursor");
  }

  @Override
  public Stream<Object> keyStream() {
    return StreamSupport.stream(
        new Spliterator<Object>() {
          private int nextPartition = 1;
          private HashTable<Object, Object> hashTable;
          private int nextEntriesIndex;
          private HashTable.Entry<Object, Object>[] entries;

          {
            if (partitions == null || partitions.isEmpty())
            //noinspection unchecked
            {
              entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            } else {
              hashTable = partitions.get(0);
              HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
              if (firstEntry == null)
              //noinspection unchecked
              {
                entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
              } else {
                entries = hashTable.ceilingEntries(firstEntry.key);
              }
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

              if (entries.length == 0 && nextPartition < partitions.size()) {
                // GET NEXT PARTITION
                hashTable = partitions.get(nextPartition++);
                HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
                if (firstEntry == null)
                //noinspection unchecked
                {
                  entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
                } else {
                  entries = hashTable.ceilingEntries(firstEntry.key);
                }
              }
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
  public Stream<RawPair<Object, RID>> iterateEntriesBetween(
      DatabaseSessionInternal session, final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesMajor(
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
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
  public boolean acquireAtomicExclusiveLock(final Object key) {
    getPartition(key).acquireAtomicExclusiveLock();
    return false;
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return getPartition(key).getName();
  }

  @Override
  public void updateUniqueIndexVersion(final Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(final Object key) {
    return 0; // not implemented
  }

  private HashTable<Object, Object> getPartition(final Object iKey) {
    final int partitionId =
        Optional.ofNullable(iKey)
            .map(key -> strategy.getPartitionsId(key, partitionSize))
            .orElse(0);
    return partitions.get(partitionId);
  }

  private static final class HashTableSpliterator implements Spliterator<RawPair<Object, RID>> {

    private int nextEntriesIndex;
    private HashTable.Entry<Object, Object>[] entries;
    private final IndexEngineValuesTransformer valuesTransformer;

    private Iterator<RID> currentIterator = new EmptyIterator<>();
    private Object currentKey;
    private final HashTable hashTable;

    private HashTableSpliterator(
        IndexEngineValuesTransformer valuesTransformer, HashTable hashTable) {
      this.valuesTransformer = valuesTransformer;
      this.hashTable = hashTable;

      @SuppressWarnings("unchecked")
      HashTable.Entry<Object, Object> firstEntry = hashTable.firstEntry();
      if (firstEntry == null) {
        //noinspection unchecked
        entries = CommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
      } else {
        //noinspection unchecked
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
          //noinspection unchecked
          entries = hashTable.higherEntries(entries[entries.length - 1].key);

          nextEntriesIndex = 0;
        }
      }

      if (currentIterator != null) {
        final Identifiable identifiable = currentIterator.next();
        action.accept(new RawPair<>(currentKey, identifiable.getIdentity()));
        return true;
      }

      currentIterator = null;
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
  }

  public boolean hasRidBagTreesSupport() {
    return true;
  }
}
