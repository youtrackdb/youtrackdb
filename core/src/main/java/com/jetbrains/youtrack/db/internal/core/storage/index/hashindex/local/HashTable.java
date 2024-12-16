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
package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import java.io.IOException;
import java.util.Comparator;

/**
 *
 */
public interface HashTable<K, V> {

  void create(
      AtomicOperation atomicOperation,
      BinarySerializer<K> keySerializer,
      BinarySerializer<V> valueSerializer,
      PropertyType[] keyTypes,
      HashFunction<K> keyHashFunction,
      boolean nullKeyIsSupported)
      throws IOException;

  V get(K key);

  /**
   * Puts the given value under the given key into this hash table. Validates the operation using
   * the provided validator.
   *
   * @param atomicOperation
   * @param key             the key to put the value under.
   * @param value           the value to put.
   * @param validator       the operation validator.
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   * @see IndexEngineValidator#validate(Object, Object, Object)
   */
  boolean validatedPut(
      AtomicOperation atomicOperation, K key, V value, IndexEngineValidator<K, V> validator)
      throws IOException;

  void put(AtomicOperation atomicOperation, K key, V value) throws IOException;

  V remove(AtomicOperation atomicOperation, K key) throws IOException;

  Entry<K, V>[] higherEntries(K key);

  Entry<K, V>[] higherEntries(K key, int limit);

  void load(
      String name,
      PropertyType[] keyTypes,
      boolean nullKeyIsSupported,
      HashFunction<K> keyHashFunction,
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer);

  Entry<K, V>[] ceilingEntries(K key);

  Entry<K, V> firstEntry();

  Entry<K, V> lastEntry();

  Entry<K, V>[] lowerEntries(K key);

  Entry<K, V>[] floorEntries(K key);

  long size();

  void close();

  void delete(AtomicOperation atomicOperation) throws IOException;

  void flush();

  boolean isNullKeyIsSupported();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * hash table.
   */
  void acquireAtomicExclusiveLock();

  String getName();

  final class BucketPath {

    public final BucketPath parent;
    public final int hashMapOffset;
    public final int itemIndex;
    public final int nodeIndex;
    public final int nodeGlobalDepth;
    public final int nodeLocalDepth;

    public BucketPath(
        BucketPath parent,
        int hashMapOffset,
        int itemIndex,
        int nodeIndex,
        int nodeLocalDepth,
        int nodeGlobalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }

  final class BucketSplitResult {

    public final long updatedBucketPointer;
    public final long newBucketPointer;
    public final int newDepth;

    public BucketSplitResult(long updatedBucketPointer, long newBucketPointer, int newDepth) {
      this.updatedBucketPointer = updatedBucketPointer;
      this.newBucketPointer = newBucketPointer;
      this.newDepth = newDepth;
    }
  }

  final class NodeSplitResult {

    public final long[] newNode;
    public final boolean allLeftHashMapsEqual;
    public final boolean allRightHashMapsEqual;

    public NodeSplitResult(
        long[] newNode, boolean allLeftHashMapsEqual, boolean allRightHashMapsEqual) {
      this.newNode = newNode;
      this.allLeftHashMapsEqual = allLeftHashMapsEqual;
      this.allRightHashMapsEqual = allRightHashMapsEqual;
    }
  }

  final class KeyHashCodeComparator<K> implements Comparator<K> {

    private final Comparator<? super K> comparator = DefaultComparator.INSTANCE;

    private final HashFunction<K> keyHashFunction;

    public KeyHashCodeComparator(HashFunction<K> keyHashFunction) {
      this.keyHashFunction = keyHashFunction;
    }

    @Override
    public int compare(K keyOne, K keyTwo) {
      final long hashCodeOne = keyHashFunction.hashCode(keyOne);
      final long hashCodeTwo = keyHashFunction.hashCode(keyTwo);

      if (greaterThanUnsigned(hashCodeOne, hashCodeTwo)) {
        return 1;
      }
      if (lessThanUnsigned(hashCodeOne, hashCodeTwo)) {
        return -1;
      }

      return comparator.compare(keyOne, keyTwo);
    }

    private static boolean lessThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
    }

    private static boolean greaterThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
    }
  }

  class RawEntry {

    public final byte[] key;
    public final byte[] value;
    public final long hashCode;

    public RawEntry(byte[] key, byte[] value, long hashCode) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
    }
  }

  class Entry<K, V> {

    public final K key;
    public final V value;
    public final long hashCode;

    public Entry(K key, V value, long hashCode) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
    }
  }
}
