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

import com.jetbrains.youtrack.db.internal.common.serialization.BinaryConverter;
import com.jetbrains.youtrack.db.internal.common.serialization.BinaryConverterFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serializer for {@link Float} type.
 *
 * @since 18.01.12
 */
public class FloatSerializer implements BinarySerializer<Float> {

  public static final byte ID = 7;

  /**
   * size of float value in bytes
   */
  public static final int FLOAT_SIZE = 4;

  private static final BinaryConverter CONVERTER = BinaryConverterFactory.getConverter();
  public static final FloatSerializer INSTANCE = new FloatSerializer();

  public int getObjectSize(Float object, Object... hints) {
    return FLOAT_SIZE;
  }

  public void serialize(Float object, byte[] stream, int startPosition, Object... hints) {
    IntegerSerializer.INSTANCE.serializeLiteral(
        Float.floatToIntBits(object), stream, startPosition);
  }

  public Float deserialize(final byte[] stream, final int startPosition) {
    return Float.intBitsToFloat(
        IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return FLOAT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return FLOAT_SIZE;
  }

  @Override
  public void serializeNativeObject(
      Float object, byte[] stream, int startPosition, Object... hints) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  @Override
  public Float deserializeNativeObject(byte[] stream, int startPosition) {
    checkBoundaries(stream, startPosition);

    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public void serializeNative(final float object, final byte[] stream, final int startPosition) {
    checkBoundaries(stream, startPosition);

    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  public float deserializeNative(final byte[] stream, final int startPosition) {
    checkBoundaries(stream, startPosition);

    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return FLOAT_SIZE;
  }

  @Override
  public Float preprocess(final Float value, final Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(Float object, ByteBuffer buffer, Object... hints) {
    buffer.putInt(Float.floatToIntBits(object));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Float deserializeFromByteBufferObject(ByteBuffer buffer) {
    return Float.intBitsToFloat(buffer.getInt());
  }

  @Override
  public Float deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    return Float.intBitsToFloat(buffer.getInt(offset));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return FLOAT_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Float deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    return Float.intBitsToFloat(walChanges.getIntValue(buffer, offset));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return FLOAT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return FLOAT_SIZE;
  }

  private static void checkBoundaries(byte[] stream, int startPosition) {
    if (startPosition + FLOAT_SIZE > stream.length) {
      throw new IllegalStateException(
          "Requested stream size is "
              + (startPosition + FLOAT_SIZE)
              + " but provided stream has size "
              + stream.length);
    }
  }
}
