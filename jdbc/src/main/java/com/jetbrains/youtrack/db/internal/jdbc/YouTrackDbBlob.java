/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.jetbrains.youtrack.db.internal.jdbc;

import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.api.record.Blob;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class YouTrackDbBlob implements java.sql.Blob {

  private final List<byte[]> binaryDataChunks;

  private long length;

  private byte[] currentChunk;

  private int currentChunkIndex;

  protected YouTrackDbBlob(Blob binaryDataChunk) throws IllegalArgumentException {
    this(Collections.singletonList(binaryDataChunk));
  }

  protected YouTrackDbBlob(List<Blob> binaryDataChunks) throws IllegalArgumentException {
    this.binaryDataChunks = new ArrayList<>(binaryDataChunks.size());
    for (var binaryDataChunk : binaryDataChunks) {
      if (binaryDataChunk == null) {
        throw new IllegalArgumentException("The binary data chunks list cannot hold null chunks");
      } else if (((RecordAbstract) binaryDataChunk).getSize() == 0) {
        throw new IllegalArgumentException("The binary data chunks list cannot hold empty chunks");
      } else {

        this.binaryDataChunks.add(((RecordAbstract) binaryDataChunk).toStream());
      }
    }
    this.length = calculateLength();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#length()
   */
  public long length() throws SQLException {
    return this.length;
  }

  private long calculateLength() {
    long length = 0;
    for (var binaryDataChunk : binaryDataChunks) {
      length += binaryDataChunk.length;
    }
    return length;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#getBytes(long, int)
   */
  public byte[] getBytes(long pos, int length) throws SQLException {
    if (pos < 1) {
      throw new SQLException(
          "The position of the first byte in the BLOB value to be "
              + "extracted cannot be less than 1");
    }
    if (length < 0) {
      throw new SQLException(
          "The number of the consecutive bytes in the BLOB value to "
              + "be extracted cannot be a negative number");
    }

    var relativeIndex = this.getRelativeIndex(pos);

    var buffer = ByteBuffer.allocate(length);
    int j;
    for (j = 0; j < length; j++) {
      if (relativeIndex == currentChunk.length) {
        // go to the next chunk, if any...
        currentChunkIndex++;
        if (currentChunkIndex < binaryDataChunks.size()) {
          // the next chunk exists so we update the relative index and
          // the current chunk reference
          relativeIndex = 0;
          currentChunk = binaryDataChunks.get(currentChunkIndex);
        } else
        // exit from the loop: there are no more bytes to be read
        {
          break;
        }
      }
      buffer.put(currentChunk[relativeIndex]);
      relativeIndex++;
    }

    return buffer.array();
  }

  /**
   * Calculates the index within a binary chunk corresponding to the given absolute position within
   * this BLOB
   *
   * @param pos
   * @return
   */
  private int getRelativeIndex(long pos) {
    var currentSize = 0;
    currentChunkIndex = 0;

    // loop until we find the chuks holding the given position
    while (pos >= (currentSize += binaryDataChunks.get(currentChunkIndex).length)) {
      currentChunkIndex++;
    }

    currentChunk = binaryDataChunks.get(currentChunkIndex);
    currentSize -= currentChunk.length;
    // the position referred to the target binary chunk
    var relativePosition = (int) (pos - currentSize);
    // the index of the first byte to be returned
    return relativePosition - 1;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#getBinaryStream()
   */
  public InputStream getBinaryStream() throws SQLException {
    return new YouTrackDbBlobInputStream();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#position(byte[], long)
   */
  public long position(byte[] pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#position(java.sql.Blob, long)
   */
  public long position(java.sql.Blob pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#setBytes(long, byte[])
   */
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#setBytes(long, byte[], int, int)
   */
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#setBinaryStream(long)
   */
  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#truncate(long)
   */
  public void truncate(long len) throws SQLException {
    if (len < 0) {
      throw new SQLException("The length of a BLOB cannot be a negtive number.");
    }
    if (len < this.length) {
      this.length = len;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#free()
   */
  public void free() throws SQLException {
    binaryDataChunks.clear();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Blob#getBinaryStream(long, long)
   */
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    return new YouTrackDbBlobInputStream(pos, length);
  }

  private class YouTrackDbBlobInputStream extends InputStream {

    private long bytesToBeRead;

    private int positionInTheCurrentChunk;

    public YouTrackDbBlobInputStream() {
      bytesToBeRead = YouTrackDbBlob.this.length;
      positionInTheCurrentChunk = 0;
    }

    public YouTrackDbBlobInputStream(long pos, long length) {
      bytesToBeRead = length;
      positionInTheCurrentChunk = YouTrackDbBlob.this.getRelativeIndex(pos);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
      if (bytesToBeRead > 0) {
        // BOUNDED READING

        // if all the bytes in the current binary chunk have been read,
        // we move to the next one
        if (positionInTheCurrentChunk == YouTrackDbBlob.this.currentChunk.length - 1) {
          // check if we've read all the available chunks
          if (YouTrackDbBlob.this.currentChunkIndex
              == YouTrackDbBlob.this.binaryDataChunks.size() - 1) {
            bytesToBeRead = 0;
            // we've read the last byte of the last binary chunk!
            return -1;
          } else {
            YouTrackDbBlob.this.currentChunk =
                YouTrackDbBlob.this.binaryDataChunks.get(++YouTrackDbBlob.this.currentChunkIndex);
            positionInTheCurrentChunk = 0;
          }
        }
        bytesToBeRead--;
        return YouTrackDbBlob.this.currentChunk[positionInTheCurrentChunk++];
      }
      return -1;
    }
  }
}
