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
package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.client.remote.TreeEntry;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SBTFetchEntriesMajorResponse<K, V> implements BinaryResponse {

  private final BinarySerializer<K> keySerializer;
  private final BinarySerializer<V> valueSerializer;
  private List<Map.Entry<K, V>> list;

  public SBTFetchEntriesMajorResponse(
      BinarySerializer<K> keySerializer, BinarySerializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public SBTFetchEntriesMajorResponse(
      BinarySerializer<K> keySerializer,
      BinarySerializer<V> valueSerializer,
      List<Map.Entry<K, V>> list) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.list = list;
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    var stream = network.readBytes();
    var offset = 0;
    final var count = IntegerSerializer.INSTANCE.deserializeLiteral(stream, 0);
    offset += IntegerSerializer.INT_SIZE;
    list = new ArrayList<Map.Entry<K, V>>(count);
    for (var i = 0; i < count; i++) {
      final var resultKey = keySerializer.deserialize(stream, offset);
      offset += keySerializer.getObjectSize(stream, offset);
      final var resultValue = valueSerializer.deserialize(stream, offset);
      offset += valueSerializer.getObjectSize(stream, offset);
      list.add(new TreeEntry<K, V>(resultKey, resultValue));
    }
  }

  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    var stream =
        new byte
            [IntegerSerializer.INT_SIZE
            + list.size()
            * (keySerializer.getFixedLength() + valueSerializer.getFixedLength())];
    var offset = 0;

    IntegerSerializer.INSTANCE.serializeLiteral(list.size(), stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    for (var entry : list) {
      keySerializer.serialize(entry.getKey(), stream, offset);
      offset += keySerializer.getObjectSize(entry.getKey());

      valueSerializer.serialize(entry.getValue(), stream, offset);
      offset += valueSerializer.getObjectSize(entry.getValue());
    }

    channel.writeBytes(stream);
  }

  public List<Map.Entry<K, V>> getList() {
    return list;
  }
}
