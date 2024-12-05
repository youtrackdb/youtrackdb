package com.jetbrains.youtrack.db.internal.core.index.engine.v1;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OBinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.encryption.OEncryption;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.YTIndexException;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValuesTransformer;
import com.jetbrains.youtrack.db.internal.core.index.engine.OSingleValueIndexEngine;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueV1;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.OVersionPositionMap;
import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.OVersionPositionMapV0;
import java.io.IOException;
import java.util.stream.Stream;

public final class OCellBTreeSingleValueIndexEngine
    implements OSingleValueIndexEngine, OCellBTreeIndexEngine {

  private static final String DATA_FILE_EXTENSION = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final OCellBTreeSingleValue<Object> sbTree;
  private final OVersionPositionMap versionPositionMap;
  private final String name;
  private final int id;
  private final OAbstractPaginatedStorage storage;

  public OCellBTreeSingleValueIndexEngine(
      int id, String name, OAbstractPaginatedStorage storage, int version) {
    this.name = name;
    this.id = id;
    this.storage = storage;

    if (version < 3) {
      this.sbTree =
          new CellBTreeSingleValueV1<>(
              name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else if (version == 3 || version == 4) {
      this.sbTree =
          new CellBTreeSingleValueV3<>(
              name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else {
      throw new IllegalStateException("Invalid tree version " + version);
    }
    versionPositionMap =
        new OVersionPositionMapV0(
            storage, name, name + DATA_FILE_EXTENSION, OVersionPositionMap.DEF_EXTENSION);
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(OIndexMetadata metadata) {
  }

  @Override
  public void flush() {
  }

  @Override
  public String getName() {
    return name;
  }

  public void create(OAtomicOperation atomicOperation, IndexEngineData data) throws IOException {

    OBinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());

    final OEncryption encryption =
        OAbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    try {
      sbTree.create(
          atomicOperation, keySerializer, data.getKeyTypes(), data.getKeySize(), encryption);
      versionPositionMap.create(atomicOperation);
    } catch (IOException e) {
      throw YTException.wrapException(new YTIndexException("Error of creation of index " + name),
          e);
    }
  }

  @Override
  public void delete(final OAtomicOperation atomicOperation) {
    try {
      doClearTree(atomicOperation);
      sbTree.delete(atomicOperation);
      versionPositionMap.delete(atomicOperation);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearTree(OAtomicOperation atomicOperation) throws IOException {
    try (Stream<Object> stream = sbTree.keyStream()) {
      stream.forEach(
          (key) -> {
            try {
              sbTree.remove(atomicOperation, key);
            } catch (IOException e) {
              throw YTException.wrapException(new YTIndexException("Can not clear index"), e);
            }
          });
    }
    sbTree.remove(atomicOperation, null);
  }

  @Override
  public void load(IndexEngineData data) {
    final OEncryption encryption =
        OAbstractPaginatedStorage.loadEncryption(data.getEncryption(), data.getEncryptionOptions());

    String name = data.getName();
    int keySize = data.getKeySize();
    YTType[] keyTypes = data.getKeyTypes();
    OBinarySerializer keySerializer = storage.resolveObjectSerializer(data.getKeySerializedId());
    sbTree.load(name, keySize, keyTypes, keySerializer, encryption);
    try {
      versionPositionMap.open();
    } catch (final IOException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during VPM load of index " + name), e);
    }
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) {
    try {
      return sbTree.remove(atomicOperation, key) != null;
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    try {
      doClearTree(atomicOperation);
    } catch (IOException e) {
      throw YTException.wrapException(new YTIndexException("Error during clear of index " + name),
          e);
    }
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public Stream<YTRID> get(Object key) {
    final YTRID rid = sbTree.get(key);
    if (rid == null) {
      return Stream.empty();
    }
    return Stream.of(rid);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> stream(IndexEngineValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return Stream.empty();
    }
    return sbTree.iterateEntriesMajor(firstKey, true, true);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> descStream(
      IndexEngineValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return Stream.empty();
    }
    return sbTree.iterateEntriesMinor(lastKey, true, false);
  }

  @Override
  public Stream<Object> keyStream() {
    return sbTree.keyStream();
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, YTRID value) {
    try {
      sbTree.put(atomicOperation, key, value);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during insertion of key " + key + " into index " + name), e);
    }
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation,
      Object key,
      YTRID value,
      IndexEngineValidator<Object, YTRID> validator) {
    try {
      return sbTree.validatedPut(atomicOperation, key, value, validator);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during insertion of key " + key + " into index " + name), e);
    }
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesBetween(
      YTDatabaseSessionInternal session, Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return sbTree.iterateEntriesBetween(
        rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> iterateEntriesMinor(
      Object toKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer) {
    return sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder);
  }

  @Override
  public long size(final IndexEngineValuesTransformer transformer) {
    return sbTree.size();
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
}
