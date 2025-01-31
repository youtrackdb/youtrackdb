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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Serializer for {@link BigDecimal} type.
 *
 * @since 03.04.12
 */
public class DecimalSerializer implements BinarySerializer<BigDecimal> {

  public static final DecimalSerializer INSTANCE = new DecimalSerializer();
  public static final byte ID = 18;

  public int getObjectSize(BigDecimal object, Object... hints) {
    return IntegerSerializer.INT_SIZE
        + BinaryTypeSerializer.INSTANCE.getObjectSize(object.unscaledValue().toByteArray());
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return IntegerSerializer.INT_SIZE
        + BinaryTypeSerializer.INSTANCE.getObjectSize(
        stream, startPosition + IntegerSerializer.INT_SIZE);
  }

  public void serialize(BigDecimal object, byte[] stream, int startPosition, Object... hints) {
    IntegerSerializer.INSTANCE.serializeLiteral(object.scale(), stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;
    BinaryTypeSerializer.INSTANCE.serialize(
        object.unscaledValue().toByteArray(), stream, startPosition);
  }

  public BigDecimal deserialize(final byte[] stream, int startPosition) {
    final var scale = IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    final var unscaledValue = BinaryTypeSerializer.INSTANCE.deserialize(stream, startPosition);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return IntegerSerializer.INT_SIZE
        + BinaryTypeSerializer.INSTANCE.getObjectSizeNative(
        stream, startPosition + IntegerSerializer.INT_SIZE);
  }

  @Override
  public void serializeNativeObject(
      BigDecimal object, byte[] stream, int startPosition, Object... hints) {
    IntegerSerializer.INSTANCE.serializeNative(object.scale(), stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;
    BinaryTypeSerializer.INSTANCE.serializeNativeObject(
        object.unscaledValue().toByteArray(), stream, startPosition);
  }

  @Override
  public BigDecimal deserializeNativeObject(byte[] stream, int startPosition) {
    final var scale = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += IntegerSerializer.INT_SIZE;

    final var unscaledValue =
        BinaryTypeSerializer.INSTANCE.deserializeNativeObject(stream, startPosition);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public BigDecimal preprocess(BigDecimal value, Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(BigDecimal object, ByteBuffer buffer, Object... hints) {
    buffer.putInt(object.scale());
    BinaryTypeSerializer.INSTANCE.serializeInByteBufferObject(
        object.unscaledValue().toByteArray(), buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal deserializeFromByteBufferObject(ByteBuffer buffer) {
    final var scale = buffer.getInt();
    final var unscaledValue =
        BinaryTypeSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  @Override
  public BigDecimal deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final var scale = buffer.getInt(offset);
    offset += Integer.BYTES;

    final var unscaledValue =
        BinaryTypeSerializer.INSTANCE.deserializeFromByteBufferObject(offset, buffer);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    buffer.position(buffer.position() + IntegerSerializer.INT_SIZE);
    return IntegerSerializer.INT_SIZE
        + BinaryTypeSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return Integer.BYTES
        + BinaryTypeSerializer.INSTANCE.getObjectSizeInByteBuffer(offset + Integer.BYTES, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final var scale = walChanges.getIntValue(buffer, offset);
    offset += IntegerSerializer.INT_SIZE;

    final var unscaledValue =
        BinaryTypeSerializer.INSTANCE.deserializeFromByteBufferObject(buffer, walChanges, offset);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return IntegerSerializer.INT_SIZE
        + BinaryTypeSerializer.INSTANCE.getObjectSizeInByteBuffer(
        buffer, walChanges, offset + IntegerSerializer.INT_SIZE);
  }
}
