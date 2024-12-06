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

package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v3;

import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import java.io.IOException;

/**
 * @since 5/14/14
 */
public class DirectoryPage extends DurablePage {

  private static final int ITEMS_OFFSET = NEXT_FREE_POSITION;

  public static final int NODES_PER_PAGE =
      (GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 - ITEMS_OFFSET)
          / HashTableDirectory.BINARY_LEVEL_SIZE;

  private final CacheEntry entry;

  DirectoryPage(CacheEntry cacheEntry, CacheEntry entry) {
    super(cacheEntry);
    this.entry = entry;
  }

  public CacheEntry getEntry() {
    return entry;
  }

  void setMaxLeftChildDepth(int localNodeIndex, byte maxLeftChildDepth) {
    int offset = getItemsOffset() + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE;
    setByteValue(offset, maxLeftChildDepth);
  }

  byte getMaxLeftChildDepth(int localNodeIndex) {
    int offset = getItemsOffset() + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE;
    return getByteValue(offset);
  }

  void setMaxRightChildDepth(int localNodeIndex, byte maxRightChildDepth) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + ByteSerializer.BYTE_SIZE;
    setByteValue(offset, maxRightChildDepth);
  }

  byte getMaxRightChildDepth(int localNodeIndex) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + ByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  void setNodeLocalDepth(int localNodeIndex, byte nodeLocalDepth) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 2 * ByteSerializer.BYTE_SIZE;
    setByteValue(offset, nodeLocalDepth);
  }

  byte getNodeLocalDepth(int localNodeIndex) {
    int offset =
        getItemsOffset()
            + localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 2 * ByteSerializer.BYTE_SIZE;
    return getByteValue(offset);
  }

  void setPointer(int localNodeIndex, int index, long pointer) throws IOException {
    int offset =
        getItemsOffset()
            + (localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 3 * ByteSerializer.BYTE_SIZE)
            + index * HashTableDirectory.ITEM_SIZE;

    setLongValue(offset, pointer);
  }

  public long getPointer(int localNodeIndex, int index) {
    int offset =
        getItemsOffset()
            + (localNodeIndex * HashTableDirectory.BINARY_LEVEL_SIZE
            + 3 * ByteSerializer.BYTE_SIZE)
            + index * HashTableDirectory.ITEM_SIZE;

    return getLongValue(offset);
  }

  protected int getItemsOffset() {
    return ITEMS_OFFSET;
  }
}
