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
package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol;
import com.jetbrains.youtrack.db.internal.core.serialization.SerializableStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class PhysicalPosition implements SerializableStream, Externalizable {

  private static final int binarySize =
      BinaryProtocol.SIZE_LONG
          + BinaryProtocol.SIZE_BYTE
          + BinaryProtocol.SIZE_INT
          + BinaryProtocol.SIZE_INT;
  public long clusterPosition;
  public byte recordType;
  public int recordVersion = 0;
  public int recordSize;

  public PhysicalPosition() {
  }

  public PhysicalPosition(final long iClusterPosition) {
    clusterPosition = iClusterPosition;
  }

  public PhysicalPosition(final byte iRecordType) {
    recordType = iRecordType;
  }

  public PhysicalPosition(final long iClusterPosition, final int iVersion) {
    clusterPosition = iClusterPosition;
    recordVersion = iVersion;
  }

  private void copyTo(final PhysicalPosition iDest) {
    iDest.clusterPosition = clusterPosition;
    iDest.recordType = recordType;
    iDest.recordVersion = recordVersion;
    iDest.recordSize = recordSize;
  }

  public void copyFrom(final PhysicalPosition iSource) {
    iSource.copyTo(this);
  }

  @Override
  public String toString() {
    return "rid(?:"
        + clusterPosition
        + ") record(type:"
        + recordType
        + " size:"
        + recordSize
        + " v:"
        + recordVersion
        + ")";
  }

  @Override
  public SerializableStream fromStream(final byte[] iStream) throws SerializationException {
    var pos = 0;

    clusterPosition = BinaryProtocol.bytes2long(iStream);
    pos += BinaryProtocol.SIZE_LONG;

    recordType = iStream[pos];
    pos += BinaryProtocol.SIZE_BYTE;

    recordSize = BinaryProtocol.bytes2int(iStream, pos);
    pos += BinaryProtocol.SIZE_INT;

    recordVersion = BinaryProtocol.bytes2int(iStream, pos);

    return this;
  }

  @Override
  public byte[] toStream() throws SerializationException {
    final var buffer = new byte[binarySize];
    var pos = 0;

    BinaryProtocol.long2bytes(clusterPosition, buffer, pos);
    pos += BinaryProtocol.SIZE_LONG;

    buffer[pos] = recordType;
    pos += BinaryProtocol.SIZE_BYTE;

    BinaryProtocol.int2bytes(recordSize, buffer, pos);
    pos += BinaryProtocol.SIZE_INT;

    BinaryProtocol.int2bytes(recordVersion, buffer, pos);
    return buffer;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof PhysicalPosition other)) {
      return false;
    }

    return clusterPosition == other.clusterPosition
        && recordType == other.recordType
        && recordVersion == other.recordVersion
        && recordSize == other.recordSize;
  }

  @Override
  public int hashCode() {
    var result = (int) (31 * clusterPosition);
    result = 31 * result + (int) recordType;
    result = 31 * result + recordVersion;
    result = 31 * result + recordSize;
    return result;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(clusterPosition);
    out.writeByte(recordType);
    out.writeInt(recordSize);
    out.writeInt(recordVersion);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterPosition = in.readLong();
    recordType = in.readByte();
    recordSize = in.readInt();
    recordVersion = in.readInt();
  }
}
