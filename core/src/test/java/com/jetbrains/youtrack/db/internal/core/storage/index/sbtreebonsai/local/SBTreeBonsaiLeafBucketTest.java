package com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local;

import com.jetbrains.youtrack.db.internal.common.directmemory.ByteBufferPool;
import com.jetbrains.youtrack.db.internal.common.directmemory.DirectMemoryAllocator.Intention;
import com.jetbrains.youtrack.db.internal.common.directmemory.Pointer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.api.record.Identifiable;
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
public class SBTreeBonsaiLeafBucketTest {

  @Test
  public void testInitialization() throws Exception {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket =
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertFalse(treeBucket.getLeftSibling().isValid());
    Assert.assertFalse(treeBucket.getRightSibling().isValid());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
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

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testUpdateValue() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    Map<Long, Integer> keyIndexMap = new HashMap<>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
          true)) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++) {
      treeBucket.updateValue(i, new RecordId(i + 5, i + 5));
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable> entry =
          treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(
          entry,
          new SBTreeBonsaiBucket.SBTreeEntry<Long, Identifiable>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              keyIndexEntry.getKey(),
              new RecordId(keyIndexEntry.getValue() + 5, keyIndexEntry.getValue() + 5)));
      Assert.assertEquals(keyIndexEntry.getKey(), treeBucket.getKey(keyIndexEntry.getValue()));
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size()
        < 2 * SBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / LongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
          true)) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2);
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
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
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
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              keyIndexEntry.getKey(),
              new RecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue())));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
          true)) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    int itemsToDelete = originalSize / 2;
    for (int i = 0; i < itemsToDelete; i++) {
      treeBucket.remove(treeBucket.size() - 1);
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
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(
          index,
          new SBTreeBonsaiBucket.SBTreeEntry<>(
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              key,
              new RecordId(index, index)),
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
              BonsaiBucketPointer.NULL,
              BonsaiBucketPointer.NULL,
              keyIndexEntry.getKey(),
              new RecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue())));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() throws Exception {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    final BonsaiBucketPointer p = new BonsaiBucketPointer(123, 8192 * 2);
    treeBucket.setLeftSibling(p);
    Assert.assertEquals(treeBucket.getLeftSibling(), p);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() throws Exception {
    ByteBufferPool bufferPool = ByteBufferPool.instance(null);
    Pointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    CachePointer cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    SBTreeBonsaiBucket<Long, Identifiable> treeBucket =
        new SBTreeBonsaiBucket<>(
            cacheEntry, 0, true, LongSerializer.INSTANCE, LinkSerializer.INSTANCE, null);
    final BonsaiBucketPointer p = new BonsaiBucketPointer(123, 8192 * 2);
    treeBucket.setRightSibling(p);
    Assert.assertEquals(treeBucket.getRightSibling(), p);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
