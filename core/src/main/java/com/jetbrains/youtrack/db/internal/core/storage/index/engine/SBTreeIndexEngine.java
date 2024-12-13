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

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngine;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValuesTransformer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.SBTree;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v1.SBTreeV1;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v2.SBTreeV2;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.VersionPositionMap;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.VersionPositionMapV0;
import java.io.IOException;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @since 8/30/13
 */
public class SBTreeIndexEngine implements IndexEngine {

  public static final int VERSION = 2;

  public static final String DATA_FILE_EXTENSION = ".sbt";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final SBTree<Object, Object> sbTree;
  private final VersionPositionMap versionPositionMap;

  private final String name;
  private final int id;

  private final AbstractPaginatedStorage storage;

  public SBTreeIndexEngine(
      final int id, String name, AbstractPaginatedStorage storage, int version) {
    this.id = id;
    this.name = name;
    this.storage = storage;

    if (version == 1) {
      sbTree = new SBTreeV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else if (version == 2) {
      sbTree = new SBTreeV2<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else {
      throw new IllegalStateException("Invalid version of index, version = " + version);
    }
    versionPositionMap =
        new VersionPositionMapV0(
            storage, name, name + DATA_FILE_EXTENSION, VersionPositionMap.DEF_EXTENSION);
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

  @Override
  public void flush() {
  }

  public void create(AtomicOperation atomicOperation, IndexEngineData data) throws IOException {

    BinarySerializer valueSerializer =
        storage.resolveObjectSerializer(data.getValueSerializerId());
    BinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());

    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    try {
      sbTree.create(
          atomicOperation,
          keySerializer,
          valueSerializer,
          data.getKeyTypes(),
          data.getKeySize(),
          data.isNullValuesSupport(),
          encryption);
      versionPositionMap.create(atomicOperation);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during creation of index " + name), e);
    }
  }

  @Override
  public void delete(AtomicOperation atomicOperation) {
    try {
      doClearTree(atomicOperation);

      sbTree.delete(atomicOperation);
      versionPositionMap.delete(atomicOperation);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearTree(AtomicOperation atomicOperation) throws IOException {
    try (final Stream<Object> stream = sbTree.keyStream()) {
      stream.forEach(
          (key) -> {
            try {
              sbTree.remove(atomicOperation, key);
            } catch (final IOException e) {
              throw BaseException.wrapException(
                  new IndexException("Error during clearing a tree" + name), e);
            }
          });
    }

    if (sbTree.isNullPointerSupport()) {
      sbTree.remove(atomicOperation, null);
    }
  }

  @Override
  public void load(IndexEngineData data) {
    final Encryption encryption =
        AbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    sbTree.load(
        data.getName(),
        (BinarySerializer) storage.resolveObjectSerializer(data.getKeySerializedId()),
        (BinarySerializer) storage.resolveObjectSerializer(data.getValueSerializerId()),
        data.getKeyTypes(),
        data.getKeySize(),
        data.isNullValuesSupport(),
        encryption);
    try {
      versionPositionMap.open();
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during VPM load of index " + data.getName()), e);
    }
  }

  @Override
  public boolean remove(AtomicOperation atomicOperation, Object key) {
    try {
      return sbTree.remove(atomicOperation, key) != null;
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public void clear(AtomicOperation atomicOperation) {
    try {
      doClearTree(atomicOperation);
    } catch (IOException e) {
      throw BaseException.wrapException(new IndexException("Error during clear index " + name), e);
    }
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public Object get(DatabaseSessionInternal session, Object key) {
    return sbTree.get(key);
  }

  @Override
  public Stream<RawPair<Object, RID>> stream(IndexEngineValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return StreamSupport.stream(Spliterators.emptySpliterator(), false);
    }
    return convertTreeStreamToIndexStream(
        valuesTransformer, sbTree.iterateEntriesMajor(firstKey, true, true));
  }

  private static Stream<RawPair<Object, RID>> convertTreeStreamToIndexStream(
      IndexEngineValuesTransformer valuesTransformer, Stream<RawPair<Object, Object>> treeStream) {
    if (valuesTransformer == null) {
      return treeStream.map((entry) -> new RawPair<>(entry.first, (RID) entry.second));
    } else {
      //noinspection resource
      return treeStream.flatMap(
          (entry) ->
              valuesTransformer.transformFromValue(entry.second).stream()
                  .map((rid) -> new RawPair<>(entry.first, rid)));
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> descStream(
      IndexEngineValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return StreamSupport.stream(Spliterators.emptySpliterator(), false);
    }

    return convertTreeStreamToIndexStream(
        valuesTransformer, sbTree.iterateEntriesMinor(lastKey, true, false));
  }

  @Override
  public Stream<Object> keyStream() {
    return sbTree.keyStream();
  }

  @Override
  public void put(DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      Object value) {
    try {
      sbTree.put(atomicOperation, key, value);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during insertion of key " + key + " in index " + name), e);
    }
  }

  @Override
  public void update(
      DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      IndexKeyUpdater<Object> updater) {
    try {
      sbTree.update(atomicOperation, key, updater, null);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during update of key " + key + " in index " + name), e);
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
      return sbTree.validatedPut(atomicOperation, key, value, (IndexEngineValidator) validator);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new IndexException("Error during insertion of key " + key + " in index " + name), e);
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesBetween(
      DatabaseSessionInternal session, Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(
        transformer,
        sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder));
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(
        transformer, sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder));
  }

  @Override
  public Stream<RawPair<Object, RID>> iterateEntriesMinor(
      Object toKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return convertTreeStreamToIndexStream(
        transformer, sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder));
  }

  @Override
  public long size(final IndexEngineValuesTransformer transformer) {
    if (transformer == null) {
      return sbTree.size();
    } else {
      int counter = 0;

      if (sbTree.isNullPointerSupport()) {
        final Object nullValue = sbTree.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      final Object firstKey = sbTree.firstKey();
      final Object lastKey = sbTree.lastKey();

      if (firstKey != null && lastKey != null) {
        try (final Stream<RawPair<Object, Object>> stream =
            sbTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
          counter +=
              stream.mapToInt((pair) -> transformer.transformFromValue(pair.second).size()).sum();
        }
        return counter;
      }

      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    sbTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
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

  public boolean hasRidBagTreesSupport() {
    return true;
  }
}
