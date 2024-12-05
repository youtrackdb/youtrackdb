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

package com.jetbrains.youtrack.db.internal.core.record;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODirtyManager;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;

public class ORecordInternal {

  /**
   * Internal only. Fills in one shot the record.
   */
  public static RecordAbstract fill(
      final Record record,
      final YTRID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  public static void checkForBinding(Record record) {
    ((RecordAbstract) record).checkForBinding();
  }

  public static RecordAbstract fill(
      final Record record,
      final YTRID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty,
      YTDatabaseSessionInternal db) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty, db);
    return rec;
  }

  public static void fromStream(
      final Record record, final byte[] iBuffer, YTDatabaseSessionInternal db) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.fromStream(iBuffer, db);
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static RecordAbstract setIdentity(
      final Record record, final int iClusterId, final long iClusterPosition) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static RecordAbstract setIdentity(final Record record, final YTRecordId iIdentity) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /**
   * Internal only. Unsets the dirty status of the record.
   */
  public static void unsetDirty(final Record record) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.unsetDirty();
  }

  /**
   * Internal only. Sets the version.
   */
  public static void setVersion(final Record record, final int iVersion) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.setVersion(iVersion);
  }

  /**
   * Internal only. Return the record type.
   */
  public static byte getRecordType(final Record record) {
    if (record instanceof RecordAbstract) {
      return ((RecordAbstract) record).getRecordType();
    }
    final RecordAbstract rec = record.getRecord();
    return rec.getRecordType();
  }

  public static boolean isContentChanged(final Record record) {
    final RecordAbstract rec = (RecordAbstract) record;
    return rec.isContentChanged();
  }

  public static void setContentChanged(final Record record, final boolean changed) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.setContentChanged(changed);
  }

  public static void clearSource(final Record record) {
    final RecordAbstract rec = (RecordAbstract) record;
    rec.clearSource();
  }

  public static void addIdentityChangeListener(
      Record record, final OIdentityChangeListener identityChangeListener) {
    if (!(record instanceof RecordAbstract)) {
      // manage O*Delegate
      record = record.getRecord();
    }
    if (record instanceof RecordAbstract) {
      ((RecordAbstract) record).addIdentityChangeListener(identityChangeListener);
    }
  }

  public static void removeIdentityChangeListener(
      final Record record, final OIdentityChangeListener identityChangeListener) {
    ((RecordAbstract) record).removeIdentityChangeListener(identityChangeListener);
  }

  public static void onBeforeIdentityChanged(final Record record) {
    ((RecordAbstract) record).onBeforeIdentityChanged();
  }

  public static void onAfterIdentityChanged(final Record record) {
    ((RecordAbstract) record).onAfterIdentityChanged();
  }

  public static void setRecordSerializer(final Record record,
      final ORecordSerializer serializer) {
    ((RecordAbstract) record).recordFormat = serializer;
  }

  public static ODirtyManager getDirtyManager(Record record) {
    if (!(record instanceof RecordAbstract)) {
      record = record.getRecord();
    }
    return ((RecordAbstract) record).getDirtyManager();
  }

  public static void setDirtyManager(Record record, final ODirtyManager dirtyManager) {
    if (!(record instanceof RecordAbstract)) {
      record = record.getRecord();
    }
    ((RecordAbstract) record).setDirtyManager(dirtyManager);
  }

  public static void track(final RecordElement pointer, final YTIdentifiable pointed) {
    RecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof Record)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof RecordAbstract) {
      ((RecordAbstract) firstRecord).track(pointed);
    }
  }

  public static void unTrack(final RecordElement pointer, final YTIdentifiable pointed) {
    RecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof Record)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof RecordAbstract) {
      ((RecordAbstract) firstRecord).unTrack(pointed);
    }
  }

  public static ORecordSerializer getRecordSerializer(Record iRecord) {
    return ((RecordAbstract) iRecord).recordFormat;
  }
}
