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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl;

import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.bytes2long;
import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.bytes2short;
import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.long2bytes;
import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.short2bytes;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

/**
 * Serializer for {@link PropertyType#LINK}
 *
 * @since 07.02.12
 */
public class LinkSerializer implements BinarySerializer<Identifiable> {

  public static final byte ID = 9;
  private static final int CLUSTER_POS_SIZE = LongSerializer.LONG_SIZE;
  public static final int RID_SIZE = ShortSerializer.SHORT_SIZE + CLUSTER_POS_SIZE;
  public static final LinkSerializer INSTANCE = new LinkSerializer();

  public int getObjectSize(final Identifiable rid, Object... hints) {
    return RID_SIZE;
  }

  public void serialize(
      final Identifiable rid, final byte[] stream, final int startPosition, Object... hints) {
    final RID r = rid.getIdentity();
    short2bytes((short) r.getClusterId(), stream, startPosition);
    long2bytes(r.getClusterPosition(), stream, startPosition + ShortSerializer.SHORT_SIZE);
  }

  public RecordId deserialize(final byte[] stream, final int startPosition) {
    return new RecordId(
        bytes2short(stream, startPosition),
        bytes2long(stream, startPosition + ShortSerializer.SHORT_SIZE));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return RID_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return RID_SIZE;
  }

  public void serializeNativeObject(
      Identifiable rid, byte[] stream, int startPosition, Object... hints) {
    final RID r = rid.getIdentity();

    ShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used serializeNative
    LongSerializer.INSTANCE.serialize(
        r.getClusterPosition(), stream, startPosition + ShortSerializer.SHORT_SIZE);
  }

  public RecordId deserializeNativeObject(byte[] stream, int startPosition) {
    final int clusterId = ShortSerializer.INSTANCE.deserializeNative(stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used deserializeNative
    final long clusterPosition =
        LongSerializer.INSTANCE.deserialize(stream, startPosition + ShortSerializer.SHORT_SIZE);
    return new RecordId(clusterId, clusterPosition);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return RID_SIZE;
  }

  @Override
  public Identifiable preprocess(Identifiable value, Object... hints) {
    if (value == null) {
      return null;
    } else {
      return value.getIdentity();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(
      Identifiable object, ByteBuffer buffer, Object... hints) {
    final RID r = object.getIdentity();

    buffer.putShort((short) r.getClusterId());
    // Wrong implementation but needed for binary compatibility
    byte[] stream = new byte[LongSerializer.LONG_SIZE];
    LongSerializer.INSTANCE.serialize(r.getClusterPosition(), stream, 0);
    buffer.put(stream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Identifiable deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int clusterId = buffer.getShort();

    final byte[] stream = new byte[LongSerializer.LONG_SIZE];
    buffer.get(stream);
    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = LongSerializer.INSTANCE.deserialize(stream, 0);

    return new RecordId(clusterId, clusterPosition);
  }

  @Override
  public Identifiable deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final int clusterId = buffer.getShort(offset);
    offset += Short.BYTES;

    final byte[] stream = new byte[LongSerializer.LONG_SIZE];
    buffer.get(offset, stream);
    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = LongSerializer.INSTANCE.deserialize(stream, 0);

    return new RecordId(clusterId, clusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return RID_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return RID_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Identifiable deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final int clusterId = walChanges.getShortValue(buffer, offset);

    // Wrong implementation but needed for binary compatibility
    final long clusterPosition =
        LongSerializer.INSTANCE.deserialize(
            walChanges.getBinaryValue(
                buffer, offset + ShortSerializer.SHORT_SIZE, LongSerializer.LONG_SIZE),
            0);

    return new RecordId(clusterId, clusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return RID_SIZE;
  }
}
