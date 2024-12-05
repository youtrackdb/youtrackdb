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
package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.serialization.OStreamable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Base interface for identifiable objects. This abstraction is required to use YTRID and Record in
 * many points.
 */
public class YTPlaceholder implements YTIdentifiable, OStreamable {

  private YTRecordId rid;
  private int recordVersion;

  /**
   * Empty constructor used by serialization
   */
  public YTPlaceholder() {
  }

  public YTPlaceholder(final YTRecordId rid, final int version) {
    this.rid = rid;
    this.recordVersion = version;
  }

  public YTPlaceholder(final Record iRecord) {
    rid = (YTRecordId) iRecord.getIdentity().copy();
    recordVersion = iRecord.getVersion();
  }

  @Override
  public YTRID getIdentity() {
    return rid;
  }

  @Nonnull
  @Override
  public <T extends Record> T getRecord() {
    return rid.getRecord();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof YTPlaceholder other)) {
      return false;
    }

    return rid.equals(other.rid) && recordVersion == other.recordVersion;
  }

  @Override
  public int hashCode() {
    return rid.hashCode() + recordVersion;
  }

  @Override
  public int compareTo(YTIdentifiable o) {
    return rid.compareTo(o);
  }

  @Override
  public int compare(YTIdentifiable o1, YTIdentifiable o2) {
    return rid.compare(o1, o2);
  }

  public int getVersion() {
    return recordVersion;
  }

  @Override
  public String toString() {
    return rid.toString() + " v." + recordVersion;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    rid.toStream(out);
    out.writeInt(recordVersion);
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    var rid = new ChangeableRecordId();
    rid.fromStream(in);

    this.rid = rid.copy();
    recordVersion = in.readInt();
  }
}
