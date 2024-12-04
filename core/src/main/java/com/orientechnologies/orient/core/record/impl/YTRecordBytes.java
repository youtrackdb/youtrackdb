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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * The rawest representation of a record. It's schema less. Use this if you need to store Strings or
 * byte[] without matter about the content. Useful also to store multimedia contents and binary
 * files. The object can be reused across calls to the database by using the reset() at every
 * re-use.
 */
@SuppressWarnings({"unchecked"})
public class YTRecordBytes extends YTRecordAbstract implements YTBlob {

  private static final byte[] EMPTY_SOURCE = new byte[]{};

  public YTRecordBytes() {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  public YTRecordBytes(final YTDatabaseSessionInternal iDatabase) {
    setup(iDatabase);
    ODatabaseRecordThreadLocal.instance().set(iDatabase);
  }

  public YTRecordBytes(final YTDatabaseSessionInternal iDatabase, final byte[] iSource) {
    this(iSource);
    ODatabaseRecordThreadLocal.instance().set(iDatabase);
  }

  public YTRecordBytes(final byte[] iSource) {
    super(iSource);
    dirty = true;
    contentChanged = true;
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  public YTRecordBytes(final YTRID iRecordId) {
    recordId = (YTRecordId) iRecordId.copy();
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  public YTRecordBytes copy() {
    return (YTRecordBytes) copyTo(new YTRecordBytes());
  }

  @Override
  public YTRecordBytes fromStream(final byte[] iRecordBuffer) {
    if (dirty) {
      throw new YTDatabaseException("Cannot call fromStream() on dirty records");
    }

    checkForBinding();

    source = iRecordBuffer;
    status = ORecordElement.STATUS.LOADED;

    return this;
  }

  @Override
  public void clear() {
    clearSource();
    super.clear();
  }

  @Override
  public byte[] toStream() {
    checkForBinding();
    return source;
  }

  public byte getRecordType() {
    return RECORD_TYPE;
  }

  /**
   * Reads the input stream in memory. This is less efficient than
   * {@link #fromInputStream(InputStream, int)} because allocation is made multiple times. If you
   * already know the input size use {@link #fromInputStream(InputStream, int)}.
   *
   * @param in Input Stream, use buffered input stream wrapper to speed up reading
   * @return Buffer read from the stream. It's also the internal buffer size in bytes
   * @throws IOException
   */
  public int fromInputStream(final InputStream in) throws IOException {
    incrementLoading();
    try {
      final OMemoryStream out = new OMemoryStream();
      try {
        final byte[] buffer = new byte[OMemoryStream.DEF_SIZE];
        int readBytesCount;
        while (true) {
          readBytesCount = in.read(buffer, 0, buffer.length);
          if (readBytesCount == -1) {
            break;
          }
          out.write(buffer, 0, readBytesCount);
        }
        out.flush();
        source = out.toByteArray();
      } finally {
        out.close();
      }
      size = source.length;
      return size;
    } finally {
      decrementLoading();
    }
  }

  /**
   * Reads the input stream in memory specifying the maximum bytes to read. This is more efficient
   * than {@link #fromInputStream(InputStream)} because allocation is made only once.
   *
   * @param in      Input Stream, use buffered input stream wrapper to speed up reading
   * @param maxSize Maximum size to read
   * @return Buffer count of bytes that are read from the stream. It's also the internal buffer size
   * in bytes
   * @throws IOException if an I/O error occurs.
   */
  public int fromInputStream(final InputStream in, final int maxSize) throws IOException {
    incrementLoading();
    try {
      final byte[] buffer = new byte[maxSize];
      int totalBytesCount = 0;
      int readBytesCount;
      while (totalBytesCount < maxSize) {
        readBytesCount = in.read(buffer, totalBytesCount, buffer.length - totalBytesCount);
        if (readBytesCount == -1) {
          break;
        }
        totalBytesCount += readBytesCount;
      }

      if (totalBytesCount == 0) {
        source = EMPTY_SOURCE;
        size = 0;
      } else if (totalBytesCount == maxSize) {
        source = buffer;
        size = maxSize;
      } else {
        source = Arrays.copyOf(buffer, totalBytesCount);
        size = totalBytesCount;
      }
      return size;
    } finally {
      decrementLoading();
    }
  }

  public void toOutputStream(final OutputStream out) throws IOException {
    checkForBinding();

    if (source.length > 0) {
      out.write(source);
    }
  }
}
