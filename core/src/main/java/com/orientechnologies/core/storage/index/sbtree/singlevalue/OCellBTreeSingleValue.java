package com.orientechnologies.core.storage.index.sbtree.singlevalue;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.encryption.OEncryption;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.engine.IndexEngineValidator;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

public interface OCellBTreeSingleValue<K> {

  void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer<K> keySerializer,
      YTType[] keyTypes,
      int keySize,
      OEncryption encryption)
      throws IOException;

  YTRID get(K key);

  void put(OAtomicOperation atomicOperation, K key, YTRID value) throws IOException;

  boolean validatedPut(
      OAtomicOperation atomicOperation, K key, YTRID value,
      IndexEngineValidator<K, YTRID> validator)
      throws IOException;

  void close();

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void load(
      String name,
      int keySize,
      YTType[] keyTypes,
      OBinarySerializer<K> keySerializer,
      OEncryption encryption);

  long size();

  YTRID remove(OAtomicOperation atomicOperation, K key) throws IOException;

  Stream<ORawPair<K, YTRID>> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  Stream<ORawPair<K, YTRID>> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  Stream<K> keyStream();

  Stream<ORawPair<K, YTRID>> allEntries();

  Stream<ORawPair<K, YTRID>> iterateEntriesBetween(
      K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, boolean ascSortOrder);

  void acquireAtomicExclusiveLock();
}
