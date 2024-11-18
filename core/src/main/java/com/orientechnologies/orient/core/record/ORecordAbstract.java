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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.OEmptyRecordId;
import com.orientechnologies.orient.core.id.OImmutableRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;

@SuppressWarnings({"unchecked"})
public abstract class ORecordAbstract implements ORecord {

  public static final String BASE_FORMAT =
      "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded";
  private static final String DEFAULT_FORMAT = BASE_FORMAT + "," + "fetchPlan:*:0";
  public static final String OLD_FORMAT_WITH_LATE_TYPES = BASE_FORMAT + "," + "fetchPlan:*:0";

  protected ORecordId recordId;
  protected int recordVersion = 0;

  protected byte[] source;
  protected int size;

  protected transient ORecordSerializer recordFormat;
  protected boolean dirty = true;
  protected boolean contentChanged = true;
  protected ORecordElement.STATUS status = ORecordElement.STATUS.LOADED;

  private transient Set<OIdentityChangeListener> newIdentityChangeListeners = null;
  protected ODirtyManager dirtyManager;

  private long loadingCounter;

  public ORecordAbstract() {}

  public ORecordAbstract(final byte[] iSource) {
    source = iSource;
    size = iSource.length;
    unsetDirty();
  }

  public final ORID getIdentity() {
    return recordId;
  }

  protected final ORecordAbstract setIdentity(final ORecordId iIdentity) {
    recordId = iIdentity;
    return this;
  }

  @Override
  public ORecordElement getOwner() {
    return null;
  }

  @Nonnull
  public ORecordAbstract getRecord() {
    return this;
  }

  public boolean detach() {
    return true;
  }

  public ORecordAbstract clear() {
    checkForBinding();

    setDirty();
    return this;
  }

  /**
   * Resets the record to be reused. The record is fresh like just created.
   */
  public ORecordAbstract reset() {
    status = ORecordElement.STATUS.LOADED;
    recordVersion = 0;
    size = 0;

    source = null;
    setDirty();
    if (recordId != null) {
      recordId.reset();
    }

    return this;
  }

  public byte[] toStream() {
    checkForBinding();

    if (source == null) {
      source = recordFormat.toStream(this);
    }

    return source;
  }

  public ORecordAbstract fromStream(final byte[] iRecordBuffer) {
    if (dirty) {
      throw new ODatabaseException("Cannot call fromStream() on dirty records");
    }

    contentChanged = false;
    dirtyManager = null;
    source = iRecordBuffer;
    size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    status = ORecordElement.STATUS.LOADED;

    return this;
  }

  protected ORecordAbstract fromStream(final byte[] iRecordBuffer, ODatabaseSessionInternal db) {
    if (dirty) {
      throw new ODatabaseException("Cannot call fromStream() on dirty records");
    }

    contentChanged = false;
    dirtyManager = null;
    source = iRecordBuffer;
    size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    status = ORecordElement.STATUS.LOADED;

    return this;
  }

  public ORecordAbstract setDirty() {
    checkForBinding();

    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }

    contentChanged = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    checkForBinding();

    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }
  }

  public final boolean isDirty() {
    return dirty;
  }

  public final boolean isDirtyNoLoading() {
    return dirty;
  }

  public <RET extends ORecord> RET fromJSON(final String iSource, final String iOptions) {
    incrementLoading();
    try {
      ORecordSerializerJSON.INSTANCE.fromString(
          iSource, this, null, iOptions, false); // Add new parameter to accommodate new API,
      // nothing change
      return (RET) this;
    } finally {
      decrementLoading();
    }
  }

  public <RET extends ORecord> RET fromJSON(final String iSource) {
    incrementLoading();
    try {
      ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null);
      return (RET) this;
    } finally {
      decrementLoading();
    }
  }

  // Add New API to load record if rid exist
  public final <RET extends ORecord> RET fromJSON(final String iSource, boolean needReload) {
    incrementLoading();
    try {
      return (RET) ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, needReload);
    } finally {
      decrementLoading();
    }
  }

  public final <RET extends ORecord> RET fromJSON(final InputStream iContentResult)
      throws IOException {
    incrementLoading();
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      OIOUtils.copyStream(iContentResult, out);
      ORecordSerializerJSON.INSTANCE.fromString(out.toString(), this, null);
      return (RET) this;
    } finally {
      decrementLoading();
    }
  }

  public String toJSON() {
    checkForBinding();
    return toJSON(DEFAULT_FORMAT);
  }

  public String toJSON(final String format) {
    checkForBinding();

    return ORecordSerializerJSON.INSTANCE
        .toString(this, new StringBuilder(1024), format == null ? "" : format)
        .toString();
  }

  public void toJSON(final String format, final OutputStream stream) throws IOException {
    checkForBinding();
    stream.write(toJSON(format).getBytes());
  }

  public void toJSON(final OutputStream stream) throws IOException {
    checkForBinding();
    stream.write(toJSON().getBytes());
  }

  @Override
  public String toString() {
    return (recordId.isValid() ? recordId : "")
        + (source != null ? Arrays.toString(source) : "[]")
        + " v"
        + recordVersion;
  }

  public final int getVersion() {
    return recordVersion;
  }

  public final int getVersionNoLoad() {
    return recordVersion;
  }

  protected final void setVersion(final int iVersion) {
    recordVersion = iVersion;
  }

  public ORecordAbstract unload() {
    if (status != ORecordElement.STATUS.NOT_LOADED) {
      source = null;
      status = ORecordElement.STATUS.NOT_LOADED;
      unsetDirty();
    }

    return this;
  }

  @Override
  public boolean isUnloaded() {
    return status == ORecordElement.STATUS.NOT_LOADED;
  }

  public ORecord load() {
    if (!getIdentity().isValid()) {
      throw new ORecordNotFoundException(
          getIdentity(), "The record has no id, probably it's new or transient yet ");
    }

    return getDatabase().load(recordId);
  }

  public ODatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public static ODatabaseSessionInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  public ORecordAbstract save() {
    getDatabase().save(this);
    return this;
  }

  public ORecordAbstract save(final String iClusterName) {
    getDatabase().save(this, iClusterName);
    return this;
  }

  public void delete() {
    checkForBinding();
    //noinspection resource
    getDatabase().delete(this);
  }

  public int getSize() {
    checkForBinding();
    return size;
  }

  @Override
  public int hashCode() {
    return recordId != null ? recordId.hashCode() : 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }

    if (obj instanceof OIdentifiable) {
      return recordId.equals(((OIdentifiable) obj).getIdentity());
    }

    return false;
  }

  public int compare(final OIdentifiable iFirst, final OIdentifiable iSecond) {
    if (iFirst == null || iSecond == null) {
      return -1;
    }

    return iFirst.compareTo(iSecond);
  }

  public int compareTo(@Nonnull final OIdentifiable iOther) {
    if (recordId == null) {
      return iOther.getIdentity() == null ? 0 : 1;
    }

    return recordId.compareTo(iOther.getIdentity());
  }

  public ORecordElement.STATUS getInternalStatus() {
    return status;
  }

  @Override
  public boolean exists() {
    return getDatabase().exists(recordId);
  }

  public void setInternalStatus(final ORecordElement.STATUS iStatus) {
    this.status = iStatus;
  }

  public ORecordAbstract copyTo(final ORecordAbstract cloned) {
    checkForBinding();

    if (cloned.dirty) {
      throw new ODatabaseException("Cannot copy to dirty records");
    }

    cloned.source = source;
    cloned.size = size;
    cloned.recordId = recordId.copy();
    cloned.recordVersion = recordVersion;
    cloned.status = status;
    cloned.recordFormat = recordFormat;
    cloned.dirty = false;
    cloned.contentChanged = false;
    cloned.dirtyManager = null;

    return cloned;
  }

  protected ORecordAbstract fill(
      final ORID iRid, final int iVersion, final byte[] iBuffer, boolean iDirty) {
    if (dirty) {
      throw new ODatabaseException("Cannot call fill() on dirty records");
    }

    recordId.setClusterId(iRid.getClusterId());
    recordId.setClusterPosition(iRid.getClusterPosition());
    recordVersion = iVersion;
    status = ORecordElement.STATUS.LOADED;
    source = iBuffer;
    size = iBuffer != null ? iBuffer.length : 0;
    dirtyManager = null;

    if (source != null && source.length > 0) {
      dirty = iDirty;
      contentChanged = iDirty;
    }

    if (dirty) {
      getDirtyManager().setDirty(this);
    }

    return this;
  }

  protected ORecordAbstract fill(
      final ORID iRid,
      final int iVersion,
      final byte[] iBuffer,
      boolean iDirty,
      ODatabaseSessionInternal db) {
    if (dirty) {
      throw new ODatabaseException("Cannot call fill() on dirty records");
    }

    recordId.setClusterId(iRid.getClusterId());
    recordId.setClusterPosition(iRid.getClusterPosition());
    recordVersion = iVersion;
    status = ORecordElement.STATUS.LOADED;
    source = iBuffer;
    size = iBuffer != null ? iBuffer.length : 0;
    dirtyManager = null;

    if (source != null && source.length > 0) {
      dirty = iDirty;
      contentChanged = iDirty;
    }

    if (dirty) {
      getDirtyManager().setDirty(this);
    }

    return this;
  }

  protected final ORecordAbstract setIdentity(final int iClusterId, final long iClusterPosition) {
    if (recordId == null || recordId == OImmutableRecordId.EMPTY_RECORD_ID) {
      recordId = new ORecordId(iClusterId, iClusterPosition);
    } else {
      recordId.setClusterId(iClusterId);
      recordId.setClusterPosition(iClusterPosition);
    }
    return this;
  }

  protected void unsetDirty() {
    contentChanged = false;
    dirty = false;
    dirtyManager = null;
  }

  protected abstract byte getRecordType();

  void onBeforeIdentityChanged() {
    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners) {
        changeListener.onBeforeIdentityChange(this);
      }
    }
  }

  void onAfterIdentityChanged() {
    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners) {
        changeListener.onAfterIdentityChange(this);
      }
    }
  }

  protected static ODatabaseSessionInternal getDatabaseIfDefinedInternal() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  void addIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    if (newIdentityChangeListeners == null) {
      newIdentityChangeListeners = Collections.newSetFromMap(new WeakHashMap<>());
    }
    newIdentityChangeListeners.add(identityChangeListener);
  }

  void removeIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    if (newIdentityChangeListeners != null) {
      newIdentityChangeListeners.remove(identityChangeListener);
    }
  }

  protected void setup(ODatabaseSessionInternal db) {
    if (recordId == null) {
      recordId = new OEmptyRecordId();
    }
  }

  protected void checkForBinding() {
    assert loadingCounter >= 0;

    if (loadingCounter > 0) {
      return;
    }

    if (status == ORecordElement.STATUS.NOT_LOADED
        && ODatabaseRecordThreadLocal.instance().isDefined()) {

      if (!getIdentity().isValid()) {
        return;
      }

      throw new ODatabaseException(
          "Record "
              + getIdentity()
              + " is not bound to the current session. Please bind record to the database session"
              + " by calling : "
              + ODatabaseSession.class.getSimpleName()
              + ".bindToSession(record) before using it.");
    }
  }

  public void incrementLoading() {
    assert loadingCounter >= 0;
    loadingCounter++;
  }

  public void decrementLoading() {
    loadingCounter--;
    assert loadingCounter >= 0;
  }

  protected boolean isContentChanged() {
    return contentChanged;
  }

  protected void setContentChanged(boolean contentChanged) {
    checkForBinding();

    this.contentChanged = contentChanged;
  }

  protected void clearSource() {
    this.source = null;
  }

  protected ODirtyManager getDirtyManager() {
    if (this.dirtyManager == null) {

      this.dirtyManager = new ODirtyManager();
      if (this.getIdentity().isNew() && getOwner() == null) {
        this.dirtyManager.setDirty(this);
      }
    }
    return this.dirtyManager;
  }

  void setDirtyManager(ODirtyManager dirtyManager) {
    checkForBinding();

    if (this.dirtyManager != null && dirtyManager != null) {
      dirtyManager.merge(this.dirtyManager);
    }
    this.dirtyManager = dirtyManager;
    if (this.getIdentity().isNew() && getOwner() == null && this.dirtyManager != null) {
      this.dirtyManager.setDirty(this);
    }
  }

  protected void track(OIdentifiable id) {
    this.getDirtyManager().track(this, id);
  }

  protected void unTrack(OIdentifiable id) {
    this.getDirtyManager().unTrack(this, id);
  }

  public void resetToNew() {
    if (!recordId.isNew()) {
      throw new IllegalStateException(
          "Record id is not new " + recordId + " as expected, so record can't be reset.");
    }

    reset();
  }
}
