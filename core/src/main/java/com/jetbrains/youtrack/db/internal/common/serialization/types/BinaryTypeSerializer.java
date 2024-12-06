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
import java.util.Arrays;

/**
 * Serializer for byte arrays .
 *
 * @since 20.01.12
 */
public class BinaryTypeSerializer implements BinarySerializer<byte[]> {

  public static final BinaryTypeSerializer INSTANCE = new BinaryTypeSerializer();
  public static final byte ID = 17;

  public int getObjectSize(int length) {
    return length + IntegerSerializer.INT_SIZE;
  }

  public int getObjectSize(byte[] object, Object... hints) {
    return object.length + IntegerSerializer.INT_SIZE;
  }

  public void serialize(
      final byte[] object, final byte[] stream, final int startPosition, final Object... hints) {
    int len = object.length;
    IntegerSerializer.INSTANCE.serializeLiteral(len, stream, startPosition);
    System.arraycopy(object, 0, stream, startPosition + IntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserialize(final byte[] stream, final int startPosition) {
    final int len = IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    return Arrays.copyOfRange(
        stream,
        startPosition + IntegerSerializer.INT_SIZE,
        startPosition + IntegerSerializer.INT_SIZE + len);
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition)
        + IntegerSerializer.INT_SIZE;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition)
        + IntegerSerializer.INT_SIZE;
  }

  public void serializeNativeObject(
      byte[] object, byte[] stream, int startPosition, Object... hints) {
    final int len = object.length;
    IntegerSerializer.INSTANCE.serializeNative(len, stream, startPosition);
    System.arraycopy(object, 0, stream, startPosition + IntegerSerializer.INT_SIZE, len);
  }

  public byte[] deserializeNativeObject(byte[] stream, int startPosition) {
    final int len = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    return Arrays.copyOfRange(
        stream,
        startPosition + IntegerSerializer.INT_SIZE,
        startPosition + IntegerSerializer.INT_SIZE + len);
  }

  public byte getId() {
    return ID;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public byte[] preprocess(byte[] value, Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(byte[] object, ByteBuffer buffer, Object... hints) {
    final int len = object.length;
    buffer.putInt(len);
    buffer.put(object);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int len = buffer.getInt();
    final byte[] result = new byte[len];
    buffer.get(result);
    return result;
  }

  @Override
  public byte[] deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final int len = buffer.getInt(offset);
    offset += Integer.BYTES;

    final byte[] result = new byte[len];
    buffer.get(offset, result);

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt() + IntegerSerializer.INT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return buffer.getInt(offset) + IntegerSerializer.INT_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final int len = walChanges.getIntValue(buffer, offset);
    offset += IntegerSerializer.INT_SIZE;
    return walChanges.getBinaryValue(buffer, offset, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset) + IntegerSerializer.INT_SIZE;
  }
}
