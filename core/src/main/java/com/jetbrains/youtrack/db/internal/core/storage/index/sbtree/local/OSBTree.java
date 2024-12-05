package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local;

import com.jetbrains.youtrack.db.internal.common.serialization.types.OBinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.encryption.OEncryption;
import com.jetbrains.youtrack.db.internal.core.index.OIndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

public interface OSBTree<K, V> {

  void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      YTType[] keyTypes,
      int keySize,
      boolean nullPointerSupport,
      OEncryption encryption)
      throws IOException;

  boolean isNullPointerSupport();

  V get(K key);

  void put(OAtomicOperation atomicOperation, K key, V value) throws IOException;

  boolean validatedPut(
      OAtomicOperation atomicOperation, K key, V value, IndexEngineValidator<K, V> validator)
      throws IOException;

  boolean update(
      OAtomicOperation atomicOperation,
      K key,
      OIndexKeyUpdater<V> updater,
      IndexEngineValidator<K, V> validator)
      throws IOException;

  void close(boolean flush);

  void close();

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void load(
      String name,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      YTType[] keyTypes,
      int keySize,
      boolean nullPointerSupport,
      OEncryption encryption);

  long size();

  V remove(OAtomicOperation atomicOperation, K key) throws IOException;

  Stream<ORawPair<K, V>> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  Stream<ORawPair<K, V>> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  Stream<K> keyStream();

  Stream<ORawPair<K, V>> iterateEntriesBetween(
      K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, boolean ascSortOrder);

  void flush();

  void acquireAtomicExclusiveLock();
}
