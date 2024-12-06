package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v2;

import com.jetbrains.youtrack.db.internal.common.directmemory.ByteBufferPool;
import com.jetbrains.youtrack.db.internal.common.directmemory.DirectMemoryAllocator.Intention;
import com.jetbrains.youtrack.db.internal.common.directmemory.Pointer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntryImpl;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CachePointer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 09.08.13
 */
public class SBTreeLeafBucketV2Test {

  @Test
  public void testInitialization() {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new SBTreeBucketV2<>(cacheEntry);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling(), -1);
    Assert.assertEquals(treeBucket.getRightSibling(), -1);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * SBTreeBucketV2.MAX_PAGE_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<>();
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index), true))) {
        break;
      }
      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), LongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testUpdateValue() {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * SBTreeBucketV2.MAX_PAGE_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    Map<Long, Integer> keyIndexMap = new HashMap<>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++) {
      final byte[] rawValue = new byte[LinkSerializer.RID_SIZE];

      LinkSerializer.INSTANCE.serializeNativeObject(new RecordId(i + 5, i + 5), rawValue, 0);
      treeBucket.updateValue(i, rawValue, LongSerializer.LONG_SIZE);
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBucketV2.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), LongSerializer.INSTANCE, LinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new SBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new SBTreeValue<>(
                  false,
                  -1,
                  new RecordId(keyIndexEntry.getValue() + 5, keyIndexEntry.getValue() + 5))));
      Assert.assertEquals(
          keyIndexEntry.getKey(),
          treeBucket.getKey(keyIndexEntry.getValue(), LongSerializer.INSTANCE));
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * SBTreeBucketV2.MAX_PAGE_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index)))) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2, LongSerializer.INSTANCE, LinkSerializer.INSTANCE);
    Assert.assertEquals(treeBucket.size(), index / 2);

    index = 0;
    final Map<Long, Integer> keyIndexMap = new HashMap<>();

    Iterator<Long> keysIterator = keys.iterator();
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), LongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBucketV2.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), LongSerializer.INSTANCE, LinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new SBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new SBTreeValue<>(
                  false, -1, new RecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * SBTreeBucketV2.MAX_PAGE_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index)))) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    int itemsToDelete = originalSize / 2;
    for (int i = 0; i < itemsToDelete; i++) {
      final byte[] rawKey = treeBucket.getRawKey(i, LongSerializer.INSTANCE);
      final byte[] rawValue =
          treeBucket.getRawValue(i, LongSerializer.INSTANCE, LinkSerializer.INSTANCE);

      treeBucket.removeLeafEntry(treeBucket.size() - 1, rawKey, rawValue);
    }

    Assert.assertEquals(treeBucket.size(), originalSize - itemsToDelete);

    final Map<Long, Integer> keyIndexMap = new HashMap<>();
    Iterator<Long> keysIterator = keys.iterator();

    index = 0;
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), LongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addLeafEntry(
          index,
          LongSerializer.INSTANCE.serializeNativeAsWhole(key),
          LinkSerializer.INSTANCE.serializeNativeAsWhole(new RecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBucketV2.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), LongSerializer.INSTANCE, LinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new SBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new SBTreeValue<>(
                  false, -1, new RecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    treeBucket.setLeftSibling(123);
    Assert.assertEquals(treeBucket.getLeftSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBucketV2<Long, Identifiable> treeBucket = new SBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    treeBucket.setRightSibling(123);
    Assert.assertEquals(treeBucket.getRightSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
