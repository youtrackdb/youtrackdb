/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage;

import java.util.Arrays;
import java.util.Objects;

public final class ORawBuffer {

  public final int version;
  public final byte recordType;
  public final byte[] buffer;

  public ORawBuffer(final byte[] buffer, final int version, final byte recordType) {
    this.buffer = buffer;
    this.version = version;
    this.recordType = recordType;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ORawBuffer that = (ORawBuffer) o;

    return version == that.version
        && recordType == that.recordType
        && Objects.deepEquals(buffer, that.buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, recordType, Arrays.hashCode(buffer));
  }

  @Override
  public String toString() {
    return "ORawBuffer{"
        + "version="
        + version
        + ", recordType="
        + recordType
        + ", buffer="
        + Arrays.toString(buffer)
        + '}';
  }
}
