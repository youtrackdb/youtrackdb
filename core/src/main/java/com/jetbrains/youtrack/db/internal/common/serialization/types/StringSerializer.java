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

/**
 * Serializer for {@link String} type.
 *
 * @since 18.01.12
 */
public class StringSerializer implements BinarySerializer<String> {

  public static final StringSerializer INSTANCE = new StringSerializer();
  public static final byte ID = 13;

  public int getObjectSize(final String object, Object... hints) {
    return object.length() * 2 + IntegerSerializer.INT_SIZE;
  }

  public void serialize(
      final String object, final byte[] stream, int startPosition, Object... hints) {
    final int length = object.length();
    IntegerSerializer.INSTANCE.serializeLiteral(length, stream, startPosition);

    startPosition += IntegerSerializer.INT_SIZE;
    final char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    for (char character : stringContent) {
      stream[startPosition] = (byte) character;
      startPosition++;

      stream[startPosition] = (byte) (character >>> 8);
      startPosition++;
    }
  }

  public String deserialize(final byte[] stream, int startPosition) {
    final int len = IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    final char[] buffer = new char[len];

    startPosition += IntegerSerializer.INT_SIZE;

    for (int i = 0; i < len; i++) {
      buffer[i] =
          (char) ((0xFF & stream[startPosition]) | ((0xFF & stream[startPosition + 1]) << 8));
      startPosition += 2;
    }

    return new String(buffer);
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition) * 2
        + IntegerSerializer.INT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition) * 2
        + IntegerSerializer.INT_SIZE;
  }

  @Override
  public void serializeNativeObject(
      String object, byte[] stream, int startPosition, Object... hints) {
    int length = object.length();
    IntegerSerializer.INSTANCE.serializeNative(length, stream, startPosition);

    startPosition += IntegerSerializer.INT_SIZE;
    char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    for (char character : stringContent) {
      stream[startPosition] = (byte) character;
      startPosition++;

      stream[startPosition] = (byte) (character >>> 8);
      startPosition++;
    }
  }

  public String deserializeNativeObject(byte[] stream, int startPosition) {
    int len = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    char[] buffer = new char[len];

    startPosition += IntegerSerializer.INT_SIZE;

    for (int i = 0; i < len; i++) {
      buffer[i] =
          (char) ((0xFF & stream[startPosition]) | ((0xFF & stream[startPosition + 1]) << 8));
      startPosition += 2;
    }

    return new String(buffer);
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    throw new UnsupportedOperationException("Length of serialized string is not fixed.");
  }

  @Override
  public String preprocess(String value, Object... hints) {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(String object, ByteBuffer buffer, Object... hints) {
    int length = object.length();
    buffer.putInt(length);

    byte[] binaryData = new byte[length * 2];
    char[] stringContent = new char[length];

    object.getChars(0, length, stringContent, 0);

    int counter = 0;
    for (char character : stringContent) {
      binaryData[counter] = (byte) character;
      counter++;

      binaryData[counter] = (byte) (character >>> 8);
      counter++;
    }

    buffer.put(binaryData);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String deserializeFromByteBufferObject(ByteBuffer buffer) {
    int len = buffer.getInt();

    final char[] chars = new char[len];
    final byte[] binaryData = new byte[2 * len];
    buffer.get(binaryData);

    for (int i = 0; i < len; i++) {
      chars[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));
    }

    return new String(chars);
  }

  @Override
  public String deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    int len = buffer.getInt(offset);
    offset += IntegerSerializer.INT_SIZE;

    final char[] chars = new char[len];
    final byte[] binaryData = new byte[2 * len];
    buffer.get(offset, binaryData);

    for (int i = 0; i < len; i++) {
      chars[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));
    }

    return new String(chars);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt() * 2 + IntegerSerializer.INT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return buffer.getInt(offset) * 2 + IntegerSerializer.INT_SIZE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    int len = walChanges.getIntValue(buffer, offset);

    final char[] chars = new char[len];
    offset += IntegerSerializer.INT_SIZE;

    byte[] binaryData = walChanges.getBinaryValue(buffer, offset, 2 * len);

    for (int i = 0; i < len; i++) {
      chars[i] = (char) ((0xFF & binaryData[i << 1]) | ((0xFF & binaryData[(i << 1) + 1]) << 8));
    }

    return new String(chars);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset) * 2 + IntegerSerializer.INT_SIZE;
  }
}
