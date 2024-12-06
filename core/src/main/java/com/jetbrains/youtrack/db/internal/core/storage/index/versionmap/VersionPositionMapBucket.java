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

package com.jetbrains.youtrack.db.internal.core.storage.index.versionmap;

import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;

public final class VersionPositionMapBucket extends DurablePage {

  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = NEXT_PAGE_OFFSET + LongSerializer.LONG_SIZE;
  private static final int POSITIONS_OFFSET = SIZE_OFFSET + IntegerSerializer.INT_SIZE;

  // use int for version
  private static final int VERSION_ENTRY_SIZE = IntegerSerializer.INT_SIZE;

  public VersionPositionMapBucket(final CacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public int getVersion(final int index) {
    final int entryPosition = entryPosition(index); // ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int value = getIntValue(entryPosition);
    if (value < 0) {
      throw new StorageException(
          "Entry with index " + index + " might be deleted and can not be used.");
    }
    return value;
  }

  public void incrementVersion(final int index) {
    final int entryPosition = entryPosition(index);
    final int value = getIntValue(entryPosition);
    if (value < 0) {
      throw new StorageException(
          "Entry with index " + index + " might be deleted and can not be used.");
    }
    setIntValue(entryPosition, value + 1);
    final int newValue = getVersion(index);
    assert value + 1 == newValue;
  }

  static int entryPosition(int index) {
    return index * VERSION_ENTRY_SIZE + POSITIONS_OFFSET;
  }
}
