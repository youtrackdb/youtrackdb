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

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.CacheEntry;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;

/**
 * @since 4/25/14
 */
public final class NullBucket<V> extends DurablePage {

  private final BinarySerializer<V> valueSerializer;

  public NullBucket(CacheEntry cacheEntry, BinarySerializer<V> valueSerializer, boolean isNew) {
    super(cacheEntry);
    this.valueSerializer = valueSerializer;

    if (isNew) {
      setByteValue(NEXT_FREE_POSITION, (byte) 0);
    }
  }

  public void setValue(V value) {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    final int valueSize = valueSerializer.getObjectSize(value);

    final byte[] serializedValue = new byte[valueSize];
    valueSerializer.serializeNativeObject(value, serializedValue, 0);

    setBinaryValue(NEXT_FREE_POSITION + 1, serializedValue);
  }

  public V getValue() {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    return deserializeFromDirectMemory(valueSerializer, NEXT_FREE_POSITION + 1);
  }

  public void removeValue() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}
