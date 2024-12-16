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

package com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local;

import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import java.io.IOException;

/**
 * A system bucket for bonsai tree pages. Single per file.
 *
 * <p>Holds an information about:
 *
 * <ul>
 *   <li>head of free list
 *   <li>length of free list
 *   <li>pointer to free space
 * </ul>
 */
public final class SysBucket extends BonsaiBucketAbstract {

  private static final int SYS_MAGIC_OFFSET = WAL_POSITION_OFFSET + LongSerializer.LONG_SIZE;
  private static final int FREE_SPACE_OFFSET = SYS_MAGIC_OFFSET + ByteSerializer.BYTE_SIZE;
  private static final int FREE_LIST_HEAD_OFFSET = FREE_SPACE_OFFSET + BonsaiBucketPointer.SIZE;
  private static final int FREE_LIST_LENGTH_OFFSET =
      FREE_LIST_HEAD_OFFSET + BonsaiBucketPointer.SIZE;
  private static final int TREES_COUNT_OFFSET = FREE_LIST_LENGTH_OFFSET + LongSerializer.LONG_SIZE;

  /**
   * Magic number to check if the sys bucket is initialized.
   */
  private static final byte SYS_MAGIC = (byte) 41;

  public SysBucket(CacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() throws IOException {
    setByteValue(SYS_MAGIC_OFFSET, SYS_MAGIC);
    setBucketPointer(
        FREE_SPACE_OFFSET, new BonsaiBucketPointer(0, SBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
    setBucketPointer(FREE_LIST_HEAD_OFFSET, BonsaiBucketPointer.NULL);
    setLongValue(FREE_LIST_LENGTH_OFFSET, 0L);
    setIntValue(TREES_COUNT_OFFSET, 0);
  }

  public boolean isNotInitialized() {
    return getByteValue(SYS_MAGIC_OFFSET) != 41;
  }

  public long freeListLength() {
    return getLongValue(FREE_LIST_LENGTH_OFFSET);
  }

  public void setFreeListLength(long length) throws IOException {
    setLongValue(FREE_LIST_LENGTH_OFFSET, length);
  }

  public BonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(FREE_SPACE_OFFSET);
  }

  public void setFreeSpacePointer(BonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_SPACE_OFFSET, pointer);
  }

  public BonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(FREE_LIST_HEAD_OFFSET);
  }

  public void setFreeListHead(BonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_LIST_HEAD_OFFSET, pointer);
  }

  void incrementTreesCount() {
    final int count = getIntValue(TREES_COUNT_OFFSET);
    assert count >= 0;

    setIntValue(TREES_COUNT_OFFSET, count + 1);
  }

  void decrementTreesCount() {
    final int count = getIntValue(TREES_COUNT_OFFSET);
    assert count > 0;

    setIntValue(TREES_COUNT_OFFSET, count - 1);
  }

  int getTreesCount() {
    return getIntValue(TREES_COUNT_OFFSET);
  }
}
