/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Immutable number representing the position in WAL file (LSN).
 *
 * @since 29.04.13
 */
public class LogSequenceNumber implements Comparable<LogSequenceNumber> {

  private final long segment;
  private final int position;

  public LogSequenceNumber(final long segment, final int position) {
    this.segment = segment;
    this.position = position;
  }

  public LogSequenceNumber(final DataInput in) throws IOException {
    this.segment = in.readLong();
    this.position = in.readInt();
  }

  public long getSegment() {
    return segment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogSequenceNumber that = (LogSequenceNumber) o;

    if (segment != that.segment) {
      return false;
    }
    return position == that.position;
  }

  @Override
  public int hashCode() {
    int result = (int) (segment ^ (segment >>> 32));
    result = 31 * result + position;
    return result;
  }

  public int getPosition() {
    return position;
  }

  @Override
  public int compareTo(final LogSequenceNumber otherNumber) {
    if (segment > otherNumber.segment) {
      return 1;
    }
    if (segment < otherNumber.segment) {
      return -1;
    }

    if (position > otherNumber.position) {
      return 1;
    } else if (position < otherNumber.position) {
      return -1;
    }

    return 0;
  }

  public void toStream(final DataOutput out) throws IOException {
    out.writeLong(segment);
    out.writeInt(position);
  }

  @Override
  public String toString() {
    return "LogSequenceNumber{segment=" + segment + ", position=" + position + '}';
  }
}
