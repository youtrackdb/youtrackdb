package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v2;

import com.jetbrains.youtrack.db.internal.common.directmemory.ByteBufferPool;
import com.jetbrains.youtrack.db.internal.common.directmemory.DirectMemoryAllocator.Intention;
import com.jetbrains.youtrack.db.internal.common.directmemory.Pointer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.StringSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntryImpl;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CachePointer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 4/15/14
 */
public class SBTreeNullBucketV2Test {

  @Test
  public void testEmptyBucket() {
    var bufferPool = new ByteBufferPool(1024);
    var pointer = bufferPool.acquireDirect(true, Intention.TEST);

    var cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    var bucket = new SBTreeNullBucketV2<String>(cacheEntry);
    bucket.init();
    Assert.assertNull(bucket.getValue(StringSerializer.INSTANCE));

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddGetValue() {
    var bufferPool = new ByteBufferPool(1024);
    var pointer = bufferPool.acquireDirect(true, Intention.TEST);

    var cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    var bucket = new SBTreeNullBucketV2<String>(cacheEntry);
    bucket.init();

    bucket.setValue(
        StringSerializer.INSTANCE.serializeNativeAsWhole("test"), StringSerializer.INSTANCE);
    var treeValue = bucket.getValue(StringSerializer.INSTANCE);
    Assert.assertNotNull(treeValue);
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveValue() {
    var bufferPool = new ByteBufferPool(1024);
    var pointer = bufferPool.acquireDirect(true, Intention.TEST);

    var cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    var bucket = new SBTreeNullBucketV2<String>(cacheEntry);
    bucket.init();

    bucket.setValue(
        StringSerializer.INSTANCE.serializeNativeAsWhole("test"), StringSerializer.INSTANCE);
    bucket.removeValue(StringSerializer.INSTANCE);

    var treeValue = bucket.getValue(StringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveAddValue() {
    var bufferPool = new ByteBufferPool(1024);
    var pointer = bufferPool.acquireDirect(true, Intention.TEST);

    var cachePointer = new CachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    CacheEntry cacheEntry = new CacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    var bucket = new SBTreeNullBucketV2<String>(cacheEntry);
    bucket.init();

    bucket.setValue(
        StringSerializer.INSTANCE.serializeNativeAsWhole("test"), StringSerializer.INSTANCE);
    bucket.removeValue(StringSerializer.INSTANCE);

    var treeValue = bucket.getValue(StringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    bucket.setValue(
        StringSerializer.INSTANCE.serializeNativeAsWhole("testOne"), StringSerializer.INSTANCE);

    treeValue = bucket.getValue(StringSerializer.INSTANCE);
    Assert.assertNotNull(treeValue);
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }
}
