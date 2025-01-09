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

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Lazy implementation of LinkedHashMap. It's bound to a source Record object to keep track of
 * changes. This avoid to call the makeDirty() by hand when the map is changed.
 */
@SuppressWarnings({"serial"})
public class LinkMap extends TrackedMap<Identifiable> implements Sizeable {

  private final byte recordType;
  private RecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE multiValueStatus =
      MULTIVALUE_CONTENT_TYPE.EMPTY;
  private boolean autoConvertToRecord = true;

  public LinkMap(final RecordElement iSourceRecord) {
    super(iSourceRecord);
    this.recordType = EntityImpl.RECORD_TYPE;
  }

  public LinkMap(final EntityImpl iSourceRecord, final byte iRecordType) {
    super(iSourceRecord);
    this.recordType = iRecordType;

    if (iSourceRecord != null) {
      if (!iSourceRecord.isLazyLoad())
      // SET AS NON-LAZY LOAD THE COLLECTION TOO
      {
        autoConvertToRecord = false;
      }
    }
  }

  public LinkMap(final EntityImpl iSourceRecord, final Map<Object, Identifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) {
      putAll(iOrigin);
    }
  }

  @Override
  public boolean containsValue(final Object o) {
    return super.containsValue(o);
  }

  @Override
  public Identifiable get(final Object iKey) {
    if (iKey == null) {
      return null;
    }

    final String key = iKey.toString();

    if (autoConvertToRecord) {
      convertLink2Record(key);
    }

    return super.get(key);
  }

  @Override
  public Identifiable put(final Object key, Identifiable value) {
    if (multiValueStatus == MULTIVALUE_CONTENT_TYPE.ALL_RIDS
        && value instanceof DBRecord
        && !value.getIdentity().isNew())
    // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
    {
      value = value.getIdentity();
    } else {
      multiValueStatus = RecordMultiValueHelper.updateContentType(multiValueStatus, value);
    }

    return super.put(key, value);
  }

  @Override
  public Collection<Identifiable> values() {
    return super.values();
  }

  @Override
  public Identifiable remove(Object o) {
    final Identifiable result = super.remove(o);
    if (size() == 0) {
      multiValueStatus = MULTIVALUE_CONTENT_TYPE.EMPTY;
    }
    return result;
  }

  @Override
  public void clear() {
    super.clear();
    multiValueStatus = MULTIVALUE_CONTENT_TYPE.EMPTY;
  }

  @Override
  public String toString() {
    return RecordMultiValueHelper.toString(this);
  }

  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  public void setAutoConvertToRecord(boolean convertToRecord) {
    this.autoConvertToRecord = convertToRecord;
  }

  /**
   * Convert the item with the received key to a record.
   *
   * @param iKey Key of the item to convert
   */
  private void convertLink2Record(final Object iKey) {
    if (multiValueStatus == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS) {
      return;
    }

    final Object value;

    if (iKey instanceof RID) {
      value = iKey;
    } else {
      value = super.get(iKey);
    }

    if (value instanceof RID rid) {
      try {
        // OVERWRITE IT
        DBRecord record = rid.getRecord();
        RecordInternal.unTrack(sourceRecord, rid);
        RecordInternal.track(sourceRecord, record);
        super.putInternal(iKey, record);
      } catch (RecordNotFoundException ignore) {
        // IGNORE THIS
      }
    }
  }

  public byte getRecordType() {
    return recordType;
  }

  public Iterator<Identifiable> rawIterator() {
    return super.values().iterator();
  }

  @Override
  public int size() {
    return super.size();
  }
}
