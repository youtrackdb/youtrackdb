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
package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashTable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @since 2/17/13
 */
public final class HashIndexBucketV2<K, V> extends DurablePage {

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int DEPTH_OFFSET = FREE_POINTER_OFFSET + IntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET = DEPTH_OFFSET + ByteSerializer.BYTE_SIZE;
  private static final int HISTORY_OFFSET = SIZE_OFFSET + IntegerSerializer.INT_SIZE;

  private static final int NEXT_REMOVED_BUCKET_OFFSET =
      HISTORY_OFFSET + LongSerializer.LONG_SIZE * 64;
  private static final int POSITIONS_ARRAY_OFFSET =
      NEXT_REMOVED_BUCKET_OFFSET + LongSerializer.LONG_SIZE;

  private static final int MAX_BUCKET_SIZE_BYTES =
      GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final Comparator keyComparator = DefaultComparator.INSTANCE;

  public HashIndexBucketV2(CacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
    setIntValue(FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);
  }

  public HashTable.Entry<K, V> find(
      final K key,
      final long hashCode,
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer) {
    final int index = binarySearch(key, hashCode, keySerializer);
    if (index < 0) {
      return null;
    }

    return getEntry(index, keySerializer, valueSerializer);
  }

  private int binarySearch(
      K key, long hashCode, BinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;

      final long midHashCode = getHashCode(mid);
      final int cmp;
      if (lessThanUnsigned(midHashCode, hashCode)) {
        cmp = -1;
      } else if (greaterThanUnsigned(midHashCode, hashCode)) {
        cmp = 1;
      } else {
        final K midVal = getKey(mid, keySerializer);
        //noinspection unchecked
        cmp = keyComparator.compare(midVal, key);
      }

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  private static boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static boolean greaterThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
  }

  public HashTable.Entry<K, V> getEntry(
      final int index,
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += LongSerializer.LONG_SIZE;

    final K key;

    key = deserializeFromDirectMemory(keySerializer, entryPosition);

    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    final V value = deserializeFromDirectMemory(valueSerializer, entryPosition);
    return new HashTable.Entry<>(key, value, hashCode);
  }

  public HashTable.RawEntry getRawEntry(
      final int index,
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);

    final long hashCode = getLongValue(entryPosition);
    entryPosition += LongSerializer.LONG_SIZE;

    final byte[] key;
    final byte[] value;
    final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
    key = getBinaryValue(entryPosition, keySize);
    entryPosition += keySize;

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    value = getBinaryValue(entryPosition, valueSize);
    return new HashTable.RawEntry(key, value, hashCode);
  }

  public byte[] getRawValue(
      final int index, final int keySize, final BinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);

    // skip hash code and key
    entryPosition += LongSerializer.LONG_SIZE + keySize;
    final int rawSize = getObjectSizeInDirectMemory(valueSerializer, entryPosition);
    return getBinaryValue(entryPosition, rawSize);
  }

  /**
   * Obtains the value stored under the given index in this bucket.
   *
   * @param index the value index.
   * @return the obtained value.
   */
  public V getValue(
      final int index,
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);

    // skip hash code
    entryPosition += LongSerializer.LONG_SIZE;

    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    return deserializeFromDirectMemory(valueSerializer, entryPosition);
  }

  private long getHashCode(int index) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);
    return getLongValue(entryPosition);
  }

  public K getKey(
      final int index, final BinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);
    return deserializeFromDirectMemory(keySerializer, entryPosition + LongSerializer.LONG_SIZE);
  }

  public int getIndex(
      final long hashCode,
      final K key,
      final BinarySerializer<K> keySerializer) {
    return binarySearch(key, hashCode, keySerializer);
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public Iterator<HashTable.RawEntry> iterator(
      final BinarySerializer<K> keySerializer,
      final BinarySerializer<V> valueSerializer) {
    return new RawEntryIterator(0, keySerializer, valueSerializer);
  }

  public Iterator<HashTable.Entry<K, V>> iterator(
      int index,
      BinarySerializer<K> keySerializer,
      BinarySerializer<V> valueSerializer) {
    return new EntryIterator(index, keySerializer, valueSerializer);
  }

  public int getContentSize() {
    return POSITIONS_ARRAY_OFFSET
        + size() * IntegerSerializer.INT_SIZE
        + (MAX_BUCKET_SIZE_BYTES - getIntValue(FREE_POINTER_OFFSET));
  }

  public int updateEntry(final int index, final byte[] value, final byte[] oldValue, int keySize) {
    int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE);
    entryPosition += LongSerializer.LONG_SIZE + keySize;

    if (oldValue.length != value.length) {
      return -1;
    }

    if (DefaultComparator.INSTANCE.compare(oldValue, value) == 0) {
      return 0;
    }

    setBinaryValue(entryPosition, value);
    return 1;
  }

  public void deleteEntry(
      final int index, final long hashCode, final byte[] key, final byte[] value) {
    final int size = size();
    if (index < 0 || index >= size) {
      throw new IllegalStateException("Can not delete entry outside of border of the bucket");
    }

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int positionOffset = POSITIONS_ARRAY_OFFSET + index * IntegerSerializer.INT_SIZE;
    final int entryPosition = getIntValue(positionOffset);

    final int entrySize = key.length + value.length + LongSerializer.LONG_SIZE;

    moveData(
        positionOffset + IntegerSerializer.INT_SIZE,
        positionOffset,
        size() * IntegerSerializer.INT_SIZE - (index + 1) * IntegerSerializer.INT_SIZE);

    if (entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;
    for (int i = 0; i < size - 1; i++) {
      int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += IntegerSerializer.INT_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);
    setIntValue(SIZE_OFFSET, size - 1);
  }

  public boolean addEntry(final int index, long hashCode, byte[] key, byte[] value) {
    final int entreeSize = key.length + value.length + LongSerializer.LONG_SIZE;
    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    int size = size();
    if (index < 0 || index > size) {
      throw new IllegalStateException("Can not insert entry outside of border of bucket");
    }
    if (freePointer - entreeSize
        < POSITIONS_ARRAY_OFFSET + (size + 1) * IntegerSerializer.INT_SIZE) {
      return false;
    }

    insertEntry(hashCode, key, value, index, entreeSize);
    return true;
  }

  private void insertEntry(
      long hashCode, byte[] key, byte[] value, int insertionPoint, int entreeSize) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    int size = size();

    final int positionsOffset =
        insertionPoint * IntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET;

    moveData(
        positionsOffset,
        positionsOffset + IntegerSerializer.INT_SIZE,
        size() * IntegerSerializer.INT_SIZE - insertionPoint * IntegerSerializer.INT_SIZE);

    final int entreePosition = freePointer - entreeSize;
    setIntValue(positionsOffset, entreePosition);
    serializeEntry(hashCode, key, value, entreePosition);

    setIntValue(FREE_POINTER_OFFSET, entreePosition);
    setIntValue(SIZE_OFFSET, size + 1);
  }

  private void serializeEntry(long hashCode, byte[] key, byte[] value, int entryOffset) {
    setLongValue(entryOffset, hashCode);
    entryOffset += LongSerializer.LONG_SIZE;

    setBinaryValue(entryOffset, key);
    entryOffset += key.length;

    setBinaryValue(entryOffset, value);
  }

  public int getDepth() {
    return getByteValue(DEPTH_OFFSET);
  }

  public void setDepth(int depth) {
    setByteValue(DEPTH_OFFSET, (byte) depth);
  }

  private final class EntryIterator implements Iterator<HashTable.Entry<K, V>> {

    private int currentIndex;
    private final BinarySerializer<K> keySerializer;
    private final BinarySerializer<V> valueSerializer;

    private EntryIterator(
        int currentIndex,
        BinarySerializer<K> keySerializer,
        BinarySerializer<V> valueSerializer) {
      this.currentIndex = currentIndex;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public HashTable.Entry<K, V> next() {
      if (currentIndex >= size()) {
        throw new NoSuchElementException("Iterator was reached last entity");
      }

      final HashTable.Entry<K, V> entry =
          getEntry(currentIndex, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }

  private final class RawEntryIterator implements Iterator<HashTable.RawEntry> {

    private int currentIndex;
    private final BinarySerializer<K> keySerializer;
    private final BinarySerializer<V> valueSerializer;

    private RawEntryIterator(
        int currentIndex,
        BinarySerializer<K> keySerializer,
        BinarySerializer<V> valueSerializer) {
      this.currentIndex = currentIndex;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

    }

    @Override
    public boolean hasNext() {
      return currentIndex < size();
    }

    @Override
    public HashTable.RawEntry next() {
      if (currentIndex >= size()) {
        throw new NoSuchElementException("Iterator was reached last entity");
      }

      final HashTable.RawEntry entry =
          getRawEntry(currentIndex, keySerializer, valueSerializer);
      currentIndex++;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is not supported");
    }
  }
}
