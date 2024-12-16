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

package com.jetbrains.youtrack.db.internal.common.serialization.types;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 *
 */
public class UUIDSerializer implements BinarySerializer<UUID> {

  public static final UUIDSerializer INSTANCE = new UUIDSerializer();
  public static final int UUID_SIZE = 2 * LongSerializer.LONG_SIZE;

  @Override
  public int getObjectSize(UUID object, Object... hints) {
    return UUID_SIZE;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return UUID_SIZE;
  }

  @Override
  public void serialize(
      final UUID object, final byte[] stream, final int startPosition, final Object... hints) {
    LongSerializer.INSTANCE.serializeLiteral(
        object.getMostSignificantBits(), stream, startPosition);
    LongSerializer.INSTANCE.serializeLiteral(
        object.getLeastSignificantBits(), stream, startPosition + LongSerializer.LONG_SIZE);
  }

  @Override
  public UUID deserialize(byte[] stream, int startPosition) {
    final long mostSignificantBits =
        LongSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    final long leastSignificantBits =
        LongSerializer.INSTANCE.deserializeLiteral(
            stream, startPosition + LongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public byte getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFixedLength() {
    return LongSerializer.INSTANCE.isFixedLength();
  }

  @Override
  public int getFixedLength() {
    return UUID_SIZE;
  }

  @Override
  public void serializeNativeObject(
      final UUID object, final byte[] stream, final int startPosition, final Object... hints) {
    LongSerializer.INSTANCE.serializeNative(
        object.getMostSignificantBits(), stream, startPosition);
    LongSerializer.INSTANCE.serializeNative(
        object.getLeastSignificantBits(), stream, startPosition + LongSerializer.LONG_SIZE);
  }

  @Override
  public UUID deserializeNativeObject(final byte[] stream, final int startPosition) {
    final long mostSignificantBits =
        LongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    final long leastSignificantBits =
        LongSerializer.INSTANCE.deserializeNative(
            stream, startPosition + LongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return UUID_SIZE;
  }

  @Override
  public UUID preprocess(UUID value, Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(UUID object, ByteBuffer buffer, Object... hints) {
    buffer.putLong(object.getMostSignificantBits());
    buffer.putLong(object.getLeastSignificantBits());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UUID deserializeFromByteBufferObject(ByteBuffer buffer) {
    final long mostSignificantBits = buffer.getLong();
    final long leastSignificantBits = buffer.getLong();
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  @Override
  public UUID deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final long mostSignificantBits = buffer.getLong(offset);
    offset += Long.BYTES;

    final long leastSignificantBits = buffer.getLong(offset);

    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return UUID_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return UUID_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UUID deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final long mostSignificantBits = walChanges.getLongValue(buffer, offset);
    final long leastSignificantBits =
        walChanges.getLongValue(buffer, offset + LongSerializer.LONG_SIZE);
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return UUID_SIZE;
  }
}
