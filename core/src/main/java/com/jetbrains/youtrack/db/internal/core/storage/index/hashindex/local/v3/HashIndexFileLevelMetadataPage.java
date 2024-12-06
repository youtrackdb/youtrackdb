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
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import java.io.IOException;

/**
 * @since 5/8/14
 */
public final class HashIndexFileLevelMetadataPage extends DurablePage {

  private static final int RECORDS_COUNT_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SERIALIZER_ID_OFFSET =
      RECORDS_COUNT_OFFSET + LongSerializer.LONG_SIZE;
  private static final int VALUE_SERIALIZER_ID_OFFSET =
      KEY_SERIALIZER_ID_OFFSET + ByteSerializer.BYTE_SIZE;
  private static final int METADATA_ARRAY_OFFSET =
      VALUE_SERIALIZER_ID_OFFSET + ByteSerializer.BYTE_SIZE;

  private static final int ITEM_SIZE = ByteSerializer.BYTE_SIZE + 3 * LongSerializer.LONG_SIZE;

  HashIndexFileLevelMetadataPage(CacheEntry cacheEntry, boolean isNewPage) {
    super(cacheEntry);

    if (isNewPage) {
      for (int i = 0; i < LocalHashTableV3.HASH_CODE_SIZE; i++) {
        remove(i);
      }

      setRecordsCount(0);
      setKeySerializerId((byte) -1);
      setValueSerializerId((byte) -1);
    }
  }

  void setRecordsCount(long recordsCount) {
    setLongValue(RECORDS_COUNT_OFFSET, recordsCount);
  }

  long getRecordsCount() {
    return getLongValue(RECORDS_COUNT_OFFSET);
  }

  private void setKeySerializerId(byte keySerializerId) {
    setByteValue(KEY_SERIALIZER_ID_OFFSET, keySerializerId);
  }

  public byte getKeySerializerId() throws IOException {
    return getByteValue(KEY_SERIALIZER_ID_OFFSET);
  }

  private void setValueSerializerId(byte valueSerializerId) {
    setByteValue(VALUE_SERIALIZER_ID_OFFSET, valueSerializerId);
  }

  public byte getValueSerializerId() throws IOException {
    return getByteValue(VALUE_SERIALIZER_ID_OFFSET);
  }

  public void setFileMetadata(int index, long fileId, long bucketsCount, long tombstoneIndex) {
    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    setByteValue(offset, (byte) 1);

    offset += ByteSerializer.BYTE_SIZE;

    setLongValue(offset, fileId);
    offset += LongSerializer.LONG_SIZE;

    setLongValue(offset, bucketsCount);
    offset += LongSerializer.LONG_SIZE;

    setLongValue(offset, tombstoneIndex);
    offset += LongSerializer.LONG_SIZE;
  }

  public void setBucketsCount(int index, long bucketsCount) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += ByteSerializer.BYTE_SIZE + LongSerializer.LONG_SIZE;
    setLongValue(offset, bucketsCount);
  }

  public long getBucketsCount(int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += ByteSerializer.BYTE_SIZE + LongSerializer.LONG_SIZE;
    return getLongValue(offset);
  }

  public void setTombstoneIndex(int index, long tombstoneIndex) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += ByteSerializer.BYTE_SIZE + 2 * LongSerializer.LONG_SIZE;
    setLongValue(offset, tombstoneIndex);
  }

  public long getTombstoneIndex(int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += ByteSerializer.BYTE_SIZE + 2 * LongSerializer.LONG_SIZE;
    return getLongValue(offset);
  }

  public long getFileId(int index) {
    assert !isRemoved(index);

    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;

    offset += ByteSerializer.BYTE_SIZE;
    return getLongValue(offset);
  }

  private boolean isRemoved(int index) {
    final int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    return getByteValue(offset) == 0;
  }

  public void remove(int index) {
    int offset = METADATA_ARRAY_OFFSET + index * ITEM_SIZE;
    setByteValue(offset, (byte) 0);
  }
}
