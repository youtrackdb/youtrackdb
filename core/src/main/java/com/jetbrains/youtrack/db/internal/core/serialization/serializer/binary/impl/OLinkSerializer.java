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

import static com.jetbrains.youtrack.db.internal.core.serialization.OBinaryProtocol.bytes2long;
import static com.jetbrains.youtrack.db.internal.core.serialization.OBinaryProtocol.bytes2short;
import static com.jetbrains.youtrack.db.internal.core.serialization.OBinaryProtocol.long2bytes;
import static com.jetbrains.youtrack.db.internal.core.serialization.OBinaryProtocol.short2bytes;

import com.jetbrains.youtrack.db.internal.common.serialization.types.OBinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OLongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OShortSerializer;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

/**
 * Serializer for {@link YTType#LINK}
 *
 * @since 07.02.12
 */
public class OLinkSerializer implements OBinarySerializer<YTIdentifiable> {

  public static final byte ID = 9;
  private static final int CLUSTER_POS_SIZE = OLongSerializer.LONG_SIZE;
  public static final int RID_SIZE = OShortSerializer.SHORT_SIZE + CLUSTER_POS_SIZE;
  public static final OLinkSerializer INSTANCE = new OLinkSerializer();

  public int getObjectSize(final YTIdentifiable rid, Object... hints) {
    return RID_SIZE;
  }

  public void serialize(
      final YTIdentifiable rid, final byte[] stream, final int startPosition, Object... hints) {
    final YTRID r = rid.getIdentity();
    short2bytes((short) r.getClusterId(), stream, startPosition);
    long2bytes(r.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
  }

  public YTRecordId deserialize(final byte[] stream, final int startPosition) {
    return new YTRecordId(
        bytes2short(stream, startPosition),
        bytes2long(stream, startPosition + OShortSerializer.SHORT_SIZE));
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
      YTIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    final YTRID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used serializeNative
    OLongSerializer.INSTANCE.serialize(
        r.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
  }

  public YTRecordId deserializeNativeObject(byte[] stream, int startPosition) {
    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used deserializeNative
    final long clusterPosition =
        OLongSerializer.INSTANCE.deserialize(stream, startPosition + OShortSerializer.SHORT_SIZE);
    return new YTRecordId(clusterId, clusterPosition);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return RID_SIZE;
  }

  @Override
  public YTIdentifiable preprocess(YTIdentifiable value, Object... hints) {
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
      YTIdentifiable object, ByteBuffer buffer, Object... hints) {
    final YTRID r = object.getIdentity();

    buffer.putShort((short) r.getClusterId());
    // Wrong implementation but needed for binary compatibility
    byte[] stream = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serialize(r.getClusterPosition(), stream, 0);
    buffer.put(stream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTIdentifiable deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int clusterId = buffer.getShort();

    final byte[] stream = new byte[OLongSerializer.LONG_SIZE];
    buffer.get(stream);
    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(stream, 0);

    return new YTRecordId(clusterId, clusterPosition);
  }

  @Override
  public YTIdentifiable deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final int clusterId = buffer.getShort(offset);
    offset += Short.BYTES;

    final byte[] stream = new byte[OLongSerializer.LONG_SIZE];
    buffer.get(offset, stream);
    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(stream, 0);

    return new YTRecordId(clusterId, clusterPosition);
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
  public YTIdentifiable deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int clusterId = walChanges.getShortValue(buffer, offset);

    // Wrong implementation but needed for binary compatibility
    final long clusterPosition =
        OLongSerializer.INSTANCE.deserialize(
            walChanges.getBinaryValue(
                buffer, offset + OShortSerializer.SHORT_SIZE, OLongSerializer.LONG_SIZE),
            0);

    return new YTRecordId(clusterId, clusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return RID_SIZE;
  }
}
