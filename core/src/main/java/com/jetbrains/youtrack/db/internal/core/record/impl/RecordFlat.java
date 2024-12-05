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
package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.ORecordStringable;
import java.nio.charset.StandardCharsets;

/**
 * It's schema less. Use this if you need to store Strings at low level. The object can be reused
 * across calls to the database by using the reset() at every re-use.
 */
@SuppressWarnings({"unchecked"})
@Deprecated
public class RecordFlat extends RecordAbstract implements ORecordStringable {

  private static final long serialVersionUID = 1L;
  public static final byte RECORD_TYPE = 'f';
  protected String value;

  public RecordFlat(YTDatabaseSessionInternal iDatabase) {
    this();
    ODatabaseRecordThreadLocal.instance().set(iDatabase);
  }

  public RecordFlat(YTRID rid) {
    recordId = (YTRecordId) rid.copy();
  }

  public RecordFlat() {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  public RecordFlat value(final String iValue) {
    value = iValue;
    setDirty();
    return this;
  }

  @Override
  public RecordFlat reset() {
    super.reset();
    value = null;
    return this;
  }

  @Override
  public void unload() {
    super.unload();
    value = null;
  }

  @Override
  public void clear() {
    super.clear();
    value = null;
  }

  public RecordFlat copy() {
    RecordFlat cloned = new RecordFlat();
    cloned.source = source;
    cloned.value = value;
    cloned.recordId = recordId.copy();
    cloned.dirty = dirty;
    cloned.contentChanged = contentChanged;
    cloned.recordVersion = recordVersion;
    return cloned;
  }

  public String value() {
    if (value == null) {
      // LAZY LOADING: LOAD THE RECORD FIRST
      value = new String(source, StandardCharsets.UTF_8);
    }

    return value;
  }

  @Override
  public String toString() {
    return super.toString() + " " + value();
  }

  @Override
  public RecordAbstract fromStream(final byte[] iRecordBuffer) {
    super.fromStream(iRecordBuffer);
    value = null;
    return this;
  }

  @Override
  public byte[] toStream() {
    if (source == null && value != null) {
      source = value.getBytes(StandardCharsets.UTF_8);
    }
    return source;
  }

  public int size() {
    final String v = value();
    return v != null ? v.length() : 0;
  }

  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
