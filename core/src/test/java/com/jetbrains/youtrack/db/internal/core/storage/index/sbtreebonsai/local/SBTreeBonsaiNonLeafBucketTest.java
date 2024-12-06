package com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local;

import com.jetbrains.youtrack.db.internal.common.directmemory.ByteBufferPool;
import com.jetbrains.youtrack.db.internal.common.directmemory.DirectMemoryAllocator.Intention;
import com.jetbrains.youtrack.db.internal.common.directmemory.Pointer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
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
 * @since 12.08.13
 */
public class SBTreeBonsaiNonLeafBucketTest {

  @Test
  public void testInitialization() throws Exception {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();
    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<Long, Identifiable>(
            cacheEntry, 0, false, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());

    treeBucket =
        new SBTreeBonsaiBucket<Long, Identifiable>(
            cacheEntry, 0, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling().getPageIndex(), -1);
    Assert.assertEquals(treeBucket.getRightSibling().getPageIndex(), -1);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size()
        < 2 * SBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<Long, Identifiable>(
            cacheEntry, 0, false, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              new BonsaiBucketPointer(random.nextInt(Integer.MAX_VALUE), 8192 * 2),
              new BonsaiBucketPointer(random.nextInt(Integer.MAX_VALUE), 8192 * 2),
              key,
              null),
          true)) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    BonsaiBucketPointer prevRight = BonsaiBucketPointer.NULL;
    for (int i = 0; i < treeBucket.size(); i++) {
      SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable> entry = treeBucket.getEntry(i);

      if (prevRight.getPageIndex() > 0) {
        Assert.assertEquals(entry.leftChild, prevRight);
      }

      prevRight = entry.rightChild;
    }

    BonsaiBucketPointer prevLeft = BonsaiBucketPointer.NULL;
    for (int i = treeBucket.size() - 1; i >= 0; i--) {
      SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable> entry = treeBucket.getEntry(i);

      if (prevLeft.getPageIndex() > 0) {
        Assert.assertEquals(entry.rightChild, prevLeft);
      }

      prevLeft = entry.leftChild;
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size()
        < 2 * SBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();
    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<Long, Identifiable>(
            cacheEntry, 0, false, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              new BonsaiBucketPointer(index, 8192 * 2),
              new BonsaiBucketPointer(index + 1, 8192 * 2),
              key,
              null),
          true)) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2);
    Assert.assertEquals(treeBucket.size(), index / 2);

    index = 0;
    final Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();

    Iterator<Long> keysIterator = keys.iterator();
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(
          entry,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              new BonsaiBucketPointer(keyIndexEntry.getValue(), 8192 * 2),
              new BonsaiBucketPointer(keyIndexEntry.getValue() + 1, 8192 * 2),
              keyIndexEntry.getKey(),
              null));
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              new BonsaiBucketPointer(index, 8192 * 2),
              new BonsaiBucketPointer(index + 1, 8192 * 2),
              key,
              null),
          true)) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(
          entry,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              new BonsaiBucketPointer(keyIndexEntry.getValue(), 8192 * 2),
              new BonsaiBucketPointer(keyIndexEntry.getValue() + 1, 8192 * 2),
              keyIndexEntry.getKey(),
              null));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
