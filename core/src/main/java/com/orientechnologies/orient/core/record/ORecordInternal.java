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

package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.RecordElement;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ORecordInternal {

  /**
   * Internal only. Fills in one shot the record.
   */
  public static YTRecordAbstract fill(
      final YTRecord record,
      final YTRID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty);
    return rec;
  }

  public static void checkForBinding(YTRecord record) {
    ((YTRecordAbstract) record).checkForBinding();
  }

  public static YTRecordAbstract fill(
      final YTRecord record,
      final YTRID iRid,
      final int iVersion,
      final byte[] iBuffer,
      final boolean iDirty,
      YTDatabaseSessionInternal db) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.fill(iRid, iVersion, iBuffer, iDirty, db);
    return rec;
  }

  public static void fromStream(
      final YTRecord record, final byte[] iBuffer, YTDatabaseSessionInternal db) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.fromStream(iBuffer, db);
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static YTRecordAbstract setIdentity(
      final YTRecord record, final int iClusterId, final long iClusterPosition) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.setIdentity(iClusterId, iClusterPosition);
    return rec;
  }

  /**
   * Internal only. Changes the identity of the record.
   */
  public static YTRecordAbstract setIdentity(final YTRecord record, final YTRecordId iIdentity) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.setIdentity(iIdentity);
    return rec;
  }

  /**
   * Internal only. Unsets the dirty status of the record.
   */
  public static void unsetDirty(final YTRecord record) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.unsetDirty();
  }

  /**
   * Internal only. Sets the version.
   */
  public static void setVersion(final YTRecord record, final int iVersion) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.setVersion(iVersion);
  }

  /**
   * Internal only. Return the record type.
   */
  public static byte getRecordType(final YTRecord record) {
    if (record instanceof YTRecordAbstract) {
      return ((YTRecordAbstract) record).getRecordType();
    }
    final YTRecordAbstract rec = record.getRecord();
    return rec.getRecordType();
  }

  public static boolean isContentChanged(final YTRecord record) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    return rec.isContentChanged();
  }

  public static void setContentChanged(final YTRecord record, final boolean changed) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.setContentChanged(changed);
  }

  public static void clearSource(final YTRecord record) {
    final YTRecordAbstract rec = (YTRecordAbstract) record;
    rec.clearSource();
  }

  public static void addIdentityChangeListener(
      YTRecord record, final OIdentityChangeListener identityChangeListener) {
    if (!(record instanceof YTRecordAbstract)) {
      // manage O*Delegate
      record = record.getRecord();
    }
    if (record instanceof YTRecordAbstract) {
      ((YTRecordAbstract) record).addIdentityChangeListener(identityChangeListener);
    }
  }

  public static void removeIdentityChangeListener(
      final YTRecord record, final OIdentityChangeListener identityChangeListener) {
    ((YTRecordAbstract) record).removeIdentityChangeListener(identityChangeListener);
  }

  public static void onBeforeIdentityChanged(final YTRecord record) {
    ((YTRecordAbstract) record).onBeforeIdentityChanged();
  }

  public static void onAfterIdentityChanged(final YTRecord record) {
    ((YTRecordAbstract) record).onAfterIdentityChanged();
  }

  public static void setRecordSerializer(final YTRecord record,
      final ORecordSerializer serializer) {
    ((YTRecordAbstract) record).recordFormat = serializer;
  }

  public static ODirtyManager getDirtyManager(YTRecord record) {
    if (!(record instanceof YTRecordAbstract)) {
      record = record.getRecord();
    }
    return ((YTRecordAbstract) record).getDirtyManager();
  }

  public static void setDirtyManager(YTRecord record, final ODirtyManager dirtyManager) {
    if (!(record instanceof YTRecordAbstract)) {
      record = record.getRecord();
    }
    ((YTRecordAbstract) record).setDirtyManager(dirtyManager);
  }

  public static void track(final RecordElement pointer, final YTIdentifiable pointed) {
    RecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof YTRecord)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof YTRecordAbstract) {
      ((YTRecordAbstract) firstRecord).track(pointed);
    }
  }

  public static void unTrack(final RecordElement pointer, final YTIdentifiable pointed) {
    RecordElement firstRecord = pointer;
    while (firstRecord != null && !(firstRecord instanceof YTRecord)) {
      firstRecord = firstRecord.getOwner();
    }
    if (firstRecord instanceof YTRecordAbstract) {
      ((YTRecordAbstract) firstRecord).unTrack(pointed);
    }
  }

  public static ORecordSerializer getRecordSerializer(YTRecord iRecord) {
    return ((YTRecordAbstract) iRecord).recordFormat;
  }
}
