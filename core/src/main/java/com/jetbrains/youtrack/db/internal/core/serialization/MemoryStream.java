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
package com.jetbrains.youtrack.db.internal.core.serialization;

import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Class to parse and write buffers in very fast way.
 */
@Deprecated
public class MemoryStream extends OutputStream {

  public static final int DEF_SIZE = 1024;

  private byte[] buffer;
  private int position;
  private final Charset charset = StandardCharsets.UTF_8;

  private static final int NATIVE_COPY_THRESHOLD = 9;
  private static long metricResize = 0;

  public MemoryStream() {
    this(DEF_SIZE);
  }

  /**
   * Callee takes ownership of 'buf'.
   */
  public MemoryStream(final int initialCapacity) {
    buffer = new byte[initialCapacity];
  }

  public MemoryStream(byte[] stream) {
    buffer = stream;
  }

  /**
   * Move bytes left or right of an offset.
   *
   * @param iFrom     Starting position
   * @param iPosition Offset to the iFrom value: positive values mean move right, otherwise move
   *                  left
   */
  public void move(final int iFrom, final int iPosition) {
    if (iPosition == 0) {
      return;
    }

    final int to = iFrom + iPosition;
    final int size = iPosition > 0 ? buffer.length - to : buffer.length - iFrom;

    System.arraycopy(buffer, iFrom, buffer, to, size);
  }

  public void copyFrom(final MemoryStream iSource, final int iSize) {
    if (iSize < 0) {
      return;
    }

    assureSpaceFor(position + iSize);
    System.arraycopy(iSource.buffer, iSource.position, buffer, position, iSize);
  }

  public final void writeTo(final OutputStream out) throws IOException {
    out.write(buffer, 0, position);
  }

  public final byte[] getInternalBuffer() {
    return buffer;
  }

  /**
   * Returns the used buffer as byte[].
   *
   * @return [result.length = size()]
   */
  public final byte[] toByteArray() {
    if (position == buffer.length - 1)
    // 100% USED, RETURN THE FULL BUFFER
    {
      return buffer;
    }

    final int pos = position;

    final byte[] destinBuffer = new byte[pos];
    final byte[] sourceBuffer = buffer;

    if (pos < NATIVE_COPY_THRESHOLD) {
      System.arraycopy(sourceBuffer, 0, destinBuffer, 0, pos);
    } else {
      System.arraycopy(sourceBuffer, 0, destinBuffer, 0, pos);
    }

    return destinBuffer;
  }

  /**
   * Does not reduce the current capacity.
   */
  public final void reset() {
    position = 0;
  }

  // OutputStream:

  @Override
  public final void write(final int b) {
    assureSpaceFor(BinaryProtocol.SIZE_BYTE);
    buffer[position++] = (byte) b;
  }

  @Override
  public final void write(final byte[] iBuffer, final int iOffset, final int iLength) {
    final int pos = position;
    final int tot = pos + iLength;

    assureSpaceFor(iLength);

    final byte[] localBuffer = buffer;

    if (iLength < NATIVE_COPY_THRESHOLD) {
      if (iLength >= 0) {
        System.arraycopy(iBuffer, iOffset, localBuffer, pos, iLength);
      }
    } else {
      System.arraycopy(iBuffer, iOffset, localBuffer, pos, iLength);
    }

    position = tot;
  }

  /**
   * Equivalent to {@link #reset()}.
   */
  @Override
  public final void close() {
    reset();
  }

  public final void setAsFixed(final byte[] iContent) {
    if (iContent == null) {
      return;
    }
    write(iContent, 0, iContent.length);
  }

  /**
   * Append byte[] to the stream.
   *
   * @param iContent
   * @return The begin offset of the appended content
   * @throws IOException
   */
  public int set(final byte[] iContent) {
    if (iContent == null) {
      return -1;
    }

    final int begin = position;

    assureSpaceFor(BinaryProtocol.SIZE_INT + iContent.length);

    BinaryProtocol.int2bytes(iContent.length, buffer, position);
    position += BinaryProtocol.SIZE_INT;
    write(iContent, 0, iContent.length);

    return begin;
  }

  public void remove(final int iBegin, final int iEnd) {
    if (iBegin > iEnd) {
      throw new IllegalArgumentException("Begin is bigger than end");
    }

    if (iEnd > buffer.length) {
      throw new IndexOutOfBoundsException(
          "Position " + iEnd + " is greater than the buffer length (" + buffer.length + ")");
    }

    System.arraycopy(buffer, iEnd, buffer, iBegin, buffer.length - iEnd);
  }

  public void set(final byte iContent) {
    write(iContent);
  }

  public final int setCustom(final String iContent) {
    return set(iContent.getBytes(StandardCharsets.UTF_8));
  }

  public final int setUtf8(final String iContent) {
    return set(iContent.getBytes(charset));
  }

  public int set(final boolean iContent) {
    final int begin = position;
    write((byte) (iContent ? 1 : 0));
    return begin;
  }

  public int set(final char iContent) {
    assureSpaceFor(BinaryProtocol.SIZE_CHAR);
    final int begin = position;
    BinaryProtocol.char2bytes(iContent, buffer, position);
    position += BinaryProtocol.SIZE_CHAR;
    return begin;
  }

  public int set(final int iContent) {
    assureSpaceFor(BinaryProtocol.SIZE_INT);
    final int begin = position;
    BinaryProtocol.int2bytes(iContent, buffer, position);
    position += BinaryProtocol.SIZE_INT;
    return begin;
  }

  public int set(final long iContent) {
    assureSpaceFor(BinaryProtocol.SIZE_LONG);
    final int begin = position;
    BinaryProtocol.long2bytes(iContent, buffer, position);
    position += BinaryProtocol.SIZE_LONG;
    return begin;
  }

  public int set(final short iContent) {
    assureSpaceFor(BinaryProtocol.SIZE_SHORT);
    final int begin = position;
    BinaryProtocol.short2bytes(iContent, buffer, position);
    position += BinaryProtocol.SIZE_SHORT;
    return begin;
  }

  public int getPosition() {
    return position;
  }

  public MemoryStream setPosition(final int iPosition) {
    position = iPosition;
    return this;
  }

  private void assureSpaceFor(final int iLength) {
    final byte[] localBuffer = buffer;
    final int pos = position;
    final int capacity = position + iLength;

    final int bufferLength = localBuffer.length;

    if (bufferLength < capacity) {
      metricResize++;

      final byte[] newbuf = new byte[Math.max(bufferLength << 1, capacity)];

      if (pos < NATIVE_COPY_THRESHOLD) {
        if (pos >= 0) {
          System.arraycopy(localBuffer, 0, newbuf, 0, pos);
        }
      } else {
        System.arraycopy(localBuffer, 0, newbuf, 0, pos);
      }

      buffer = newbuf;
    }
  }

  /**
   * Jumps bytes positioning forward of passed bytes.
   *
   * @param iLength Bytes to jump
   */
  public void fill(final int iLength) {
    assureSpaceFor(iLength);
    position += iLength;
  }

  /**
   * Fills the stream from current position writing iLength times the iFiller byte
   *
   * @param iLength Bytes to jump
   * @param iFiller Byte to use to fill the space
   */
  public void fill(final int iLength, final byte iFiller) {
    assureSpaceFor(iLength);
    Arrays.fill(buffer, position, position + iLength, iFiller);
    position += iLength;
  }

  public MemoryStream jump(final int iOffset) {
    if (iOffset > buffer.length) {
      throw new IndexOutOfBoundsException(
          "Offset " + iOffset + " is greater than the buffer size " + buffer.length);
    }
    position = iOffset;
    return this;
  }

  public byte[] getAsByteArrayFixed(final int iSize) {
    if (position >= buffer.length) {
      return null;
    }

    final byte[] portion = ArrayUtils.copyOfRange(buffer, position, position + iSize);
    position += iSize;

    return portion;
  }

  /**
   * Browse the stream but just return the begin of the byte array. This is used to lazy load the
   * information only when needed.
   */
  public int getAsByteArrayOffset() {
    if (position >= buffer.length) {
      return -1;
    }

    final int begin = position;

    final int size = BinaryProtocol.bytes2int(buffer, position);
    position += BinaryProtocol.SIZE_INT + size;

    return begin;
  }

  public int read() {
    return buffer[position++];
  }

  public int read(final byte[] b) {
    return read(b, 0, b.length);
  }

  public int read(final byte[] b, final int off, final int len) {
    if (position >= buffer.length) {
      return 0;
    }

    System.arraycopy(buffer, position, b, off, len);
    position += len;

    return len;
  }

  public byte[] getAsByteArray(int iOffset) {
    if (buffer == null || iOffset >= buffer.length) {
      return null;
    }

    final int size = BinaryProtocol.bytes2int(buffer, iOffset);

    if (size == 0) {
      return null;
    }

    iOffset += BinaryProtocol.SIZE_INT;

    return ArrayUtils.copyOfRange(buffer, iOffset, iOffset + size);
  }

  public byte[] getAsByteArray() {
    if (position >= buffer.length) {
      return null;
    }

    final int size = BinaryProtocol.bytes2int(buffer, position);
    position += BinaryProtocol.SIZE_INT;

    final byte[] portion = ArrayUtils.copyOfRange(buffer, position, position + size);
    position += size;

    return portion;
  }

  /**
   * Returns the available bytes to read.
   */
  public int available() {
    return buffer.length - position;
  }

  public String getAsString() {
    if (position >= buffer.length) {
      return null;
    }

    final int size = getVariableSize();
    String str = new String(buffer, position, size, charset);
    position += size;
    return str;
  }

  public boolean getAsBoolean() {
    return buffer[position++] == 1;
  }

  public char getAsChar() {
    final char value = BinaryProtocol.bytes2char(buffer, position);
    position += BinaryProtocol.SIZE_CHAR;
    return value;
  }

  public byte getAsByte() {
    return buffer[position++];
  }

  public long getAsLong() {
    final long value = BinaryProtocol.bytes2long(buffer, position);
    position += BinaryProtocol.SIZE_LONG;
    return value;
  }

  public int getAsInteger() {
    final int value = BinaryProtocol.bytes2int(buffer, position);
    position += BinaryProtocol.SIZE_INT;
    return value;
  }

  public short getAsShort() {
    final short value = BinaryProtocol.bytes2short(buffer, position);
    position += BinaryProtocol.SIZE_SHORT;
    return value;
  }

  public byte peek() {
    return buffer[position];
  }

  public void setSource(final byte[] iBuffer) {
    buffer = iBuffer;
    position = 0;
  }

  public byte[] copy() {
    if (buffer == null) {
      return null;
    }

    final int size = position > 0 ? position : buffer.length;

    final byte[] copy = new byte[size];
    System.arraycopy(buffer, 0, copy, 0, size);
    return copy;
  }

  public int getVariableSize() {
    if (position >= buffer.length) {
      return -1;
    }

    final int size = BinaryProtocol.bytes2int(buffer, position);
    position += BinaryProtocol.SIZE_INT;

    return size;
  }

  public int getSize() {
    return buffer.length;
  }

  public final int size() {
    return position;
  }
}
