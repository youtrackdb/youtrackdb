package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

public interface BaseIndexEngine {

  int getId();

  void init(IndexMetadata metadata);

  void flush();

  void create(AtomicOperation atomicOperation, IndexEngineData data) throws IOException;

  void load(IndexEngineData data);

  void delete(AtomicOperation atomicOperation) throws IOException;

  void clear(AtomicOperation atomicOperation) throws IOException;

  void close();

  Stream<RawPair<Object, RID>> iterateEntriesBetween(
      DatabaseSessionInternal session, Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<RawPair<Object, RID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<RawPair<Object, RID>> iterateEntriesMinor(
      final Object toKey,
      final boolean isInclusive,
      boolean ascSortOrder,
      IndexEngineValuesTransformer transformer);

  Stream<RawPair<Object, RID>> stream(IndexEngineValuesTransformer valuesTransformer);

  Stream<RawPair<Object, RID>> descStream(IndexEngineValuesTransformer valuesTransformer);

  Stream<Object> keyStream();

  long size(IndexEngineValuesTransformer transformer);

  boolean hasRangeQuerySupport();

  int getEngineAPIVersion();

  String getName();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * index engine.
   *
   * <p>
   *
   * <p>If this index engine supports a more narrow locking, for example key-based sharding, it may
   * use the provided {@code key} to infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is
   * sensitive to the provided {@code key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  String getIndexNameByKey(Object key);

  void updateUniqueIndexVersion(Object key);

  int getUniqueIndexVersion(Object key);

  default boolean hasRidBagTreesSupport() {
    return false;
  }
}
