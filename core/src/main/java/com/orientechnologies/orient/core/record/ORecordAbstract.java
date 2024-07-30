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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
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
import com.orientechnologies.orient.core.storage.cluster.OOfflineClusterException;
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
  // TODO: take new format
  // public static final String DEFAULT_FORMAT = OLD_FORMAT_WITH_LATE_TYPES;
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
  protected ORecordAbstract primaryRecord;

  private long loadingCounter;

  public ORecordAbstract() {}

  public void convertToProxyRecord(ORecordAbstract primaryRecord) {
    if (dirty) {
      throw new IllegalStateException("Cannot convert dirty record to proxy record");
    }

    dirtyManager = null;
    newIdentityChangeListeners = null;
    contentChanged = false;
    size = 0;
    source = null;
    status = ORecordElement.STATUS.NOT_LOADED;
    recordVersion = -1;
    recordId = null;

    this.primaryRecord = primaryRecord;
  }

  @Override
  public boolean isProxy() {
    return primaryRecord != null;
  }

  public ORecordAbstract(final byte[] iSource) {
    source = iSource;
    size = iSource.length;
    unsetDirty();
  }

  public final ORID getIdentity() {
    if (primaryRecord != null) {
      return primaryRecord.getIdentity();
    }
    return recordId;
  }

  protected final ORecordAbstract setIdentity(final ORecordId iIdentity) {
    if (primaryRecord != null) {
      primaryRecord.setIdentity(iIdentity);
      return this;
    }
    recordId = iIdentity;
    return this;
  }

  @Override
  public ORecordElement getOwner() {
    checkForLoading();
    if (primaryRecord != null) {
      return primaryRecord.getOwner();
    }

    return null;
  }

  public ORecordAbstract getRecord() {
    checkForLoading();
    if (primaryRecord != null) {
      return primaryRecord.getRecord();
    }

    return this;
  }

  public boolean detach() {
    if (primaryRecord != null) {
      return primaryRecord.detach();
    }

    return true;
  }

  public ORecordAbstract clear() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.clear();
      return primaryRecord;
    }

    setDirty();
    return this;
  }

  /**
   * Resets the record to be reused. The record is fresh like just created.
   */
  public ORecordAbstract reset() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.reset();
      return primaryRecord;
    }

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
    checkForLoading();
    if (primaryRecord != null) {
      return primaryRecord.toStream();
    }

    if (source == null) {
      source = recordFormat.toStream(this);
    }

    return source;
  }

  public ORecordAbstract fromStream(final byte[] iRecordBuffer) {
    if (primaryRecord != null) {
      primaryRecord.fromStream(iRecordBuffer);
      return primaryRecord;
    }

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

  protected ORecordAbstract fromStream(final byte[] iRecordBuffer, ODatabaseDocumentInternal db) {
    if (primaryRecord != null) {
      primaryRecord.fromStream(iRecordBuffer, db);
      return primaryRecord;
    }

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
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.setDirty();
      return primaryRecord;
    }

    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }

    contentChanged = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.setDirtyNoChanged();
      return;
    }

    if (!dirty && status != STATUS.UNMARSHALLING) {
      dirty = true;
      source = null;
    }
  }

  public final boolean isDirty() {
    checkForLoading();
    if (primaryRecord != null) {
      return primaryRecord.isDirty();
    }

    return dirty;
  }

  public <RET extends ORecord> RET fromJSON(final String iSource, final String iOptions) {
    if (primaryRecord != null) {
      primaryRecord.fromJSON(iSource, iOptions);
      return (RET) primaryRecord;
    }

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
    if (primaryRecord != null) {
      primaryRecord.fromJSON(iSource);
      return (RET) primaryRecord;
    }

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
    if (primaryRecord != null) {
      primaryRecord.fromJSON(iSource, needReload);
      return (RET) primaryRecord;
    }

    incrementLoading();
    try {
      return (RET) ORecordSerializerJSON.INSTANCE.fromString(iSource, this, null, needReload);
    } finally {
      decrementLoading();
    }
  }

  public final <RET extends ORecord> RET fromJSON(final InputStream iContentResult)
      throws IOException {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.fromJSON(iContentResult);
      return (RET) primaryRecord;
    }

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
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.toJSON();
    }

    return toJSON(DEFAULT_FORMAT);
  }

  public String toJSON(final String format) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.toJSON(format);
    }

    return ORecordSerializerJSON.INSTANCE
        .toString(this, new StringBuilder(1024), format == null ? "" : format)
        .toString();
  }

  public void toJSON(final String format, final OutputStream stream) throws IOException {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.toJSON(format, stream);
      return;
    }

    stream.write(toJSON(format).getBytes());
  }

  public void toJSON(final OutputStream stream) throws IOException {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.toJSON(stream);
      return;
    }

    stream.write(toJSON().getBytes());
  }

  @Override
  public String toString() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return "Proxy for { " + primaryRecord + " }";
    }

    return (recordId.isValid() ? recordId : "")
        + (source != null ? Arrays.toString(source) : "[]")
        + " v"
        + recordVersion;
  }

  public final int getVersion() {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.getVersion();
    }

    // checkForLoading();
    return recordVersion;
  }

  protected final void setVersion(final int iVersion) {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.setVersion(iVersion);
      return;
    }

    recordVersion = iVersion;
  }

  public ORecordAbstract unload() {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.unload();
      return primaryRecord;
    }

    if (status != ORecordElement.STATUS.NOT_LOADED) {
      source = null;
      status = ORecordElement.STATUS.NOT_LOADED;
      unsetDirty();
    }

    return this;
  }

  @Override
  public boolean isUnloaded() {
    if (primaryRecord != null) {
      return primaryRecord.isUnloaded();
    }

    return status == ORecordElement.STATUS.NOT_LOADED;
  }

  public ORecord load() {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.load();
    }

    if (!getIdentity().isValid()) {
      throw new ORecordNotFoundException(
          getIdentity(), "The record has no id, probably it's new or transient yet ");
    }

    final ORecord result = getDatabase().load(this);

    if (result == null) {
      throw new ORecordNotFoundException(getIdentity());
    }

    return result;
  }

  public ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public static ODatabaseDocumentInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  public ORecord reload() {
    if (primaryRecord != null) {
      primaryRecord = (ORecordAbstract) primaryRecord.reload();
      return primaryRecord;
    }

    return reload(null, true, true);
  }

  public ORecord reload(final String fetchPlan) {
    if (primaryRecord != null) {
      primaryRecord = (ORecordAbstract) primaryRecord.reload(fetchPlan);
      return primaryRecord;
    }

    return reload(fetchPlan, true);
  }

  public ORecord reload(final String fetchPlan, final boolean ignoreCache) {
    if (primaryRecord != null) {
      primaryRecord = (ORecordAbstract) primaryRecord.reload(fetchPlan, ignoreCache);
      return primaryRecord;
    }

    return reload(fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reload(String fetchPlan, boolean ignoreCache, boolean force)
      throws ORecordNotFoundException {
    if (primaryRecord != null) {
      primaryRecord.reload(fetchPlan, ignoreCache, force);
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord;
    }

    if (!getIdentity().isValid()) {
      throw new ORecordNotFoundException(
          getIdentity(), "The record has no id. It is probably new or still transient");
    }

    try {
      getDatabase().reload(this, fetchPlan, ignoreCache, force);

      if (primaryRecord != null) {
        return primaryRecord;
      }

      return this;
    } catch (OOfflineClusterException | ORecordNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new ORecordNotFoundException(getIdentity()), e);
    }
  }

  public ORecordAbstract save() {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.save();
    }

    getDatabase().save(this);
    return this;
  }

  public ORecordAbstract save(final String iClusterName) {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.save(iClusterName);
    }

    getDatabase().save(this, iClusterName);
    return this;
  }

  public ORecordAbstract delete() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.delete();
    }

    //noinspection resource
    getDatabase().delete(this);
    return this;
  }

  public int getSize() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.getSize();
    }

    return size;
  }

  @Override
  public int hashCode() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.hashCode();
    }

    return recordId != null ? recordId.hashCode() : 0;
  }

  @Override
  public boolean equals(final Object obj) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.equals(obj);
    }

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
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.compare(iFirst, iSecond);
    }

    if (iFirst == null || iSecond == null) {
      return -1;
    }
    return iFirst.compareTo(iSecond);
  }

  public int compareTo(@Nonnull final OIdentifiable iOther) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.compareTo(iOther);
    }

    if (recordId == null) {
      return iOther.getIdentity() == null ? 0 : 1;
    }

    return recordId.compareTo(iOther.getIdentity());
  }

  public ORecordElement.STATUS getInternalStatus() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.getInternalStatus();
    }

    return status;
  }

  @Override
  public boolean exists() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.exists();
    }

    return getDatabase().exists(recordId);
  }

  public void setInternalStatus(final ORecordElement.STATUS iStatus) {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.setInternalStatus(iStatus);
      return;
    }

    this.status = iStatus;
  }

  public ORecordAbstract copyTo(final ORecordAbstract cloned) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.copyTo(cloned);
    }

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
    if (primaryRecord != null) {
      return primaryRecord.fill(iRid, iVersion, iBuffer, iDirty);
    }

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
      ODatabaseDocumentInternal db) {
    if (primaryRecord != null) {
      return primaryRecord.fill(iRid, iVersion, iBuffer, iDirty, db);
    }

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
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      return primaryRecord.setIdentity(iClusterId, iClusterPosition);
    }

    if (recordId == null || recordId == OImmutableRecordId.EMPTY_RECORD_ID) {
      recordId = new ORecordId(iClusterId, iClusterPosition);
    } else {
      recordId.setClusterId(iClusterId);
      recordId.setClusterPosition(iClusterPosition);
    }
    return this;
  }

  protected void unsetDirty() {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.unsetDirty();
      return;
    }

    contentChanged = false;
    dirty = false;
    dirtyManager = null;
  }

  protected abstract byte getRecordType();

  void onBeforeIdentityChanged() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.onBeforeIdentityChanged();
      return;
    }

    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners) {
        changeListener.onBeforeIdentityChange(this);
      }
    }
  }

  void onAfterIdentityChanged() {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.onAfterIdentityChanged();
      return;
    }

    if (newIdentityChangeListeners != null) {
      for (OIdentityChangeListener changeListener : newIdentityChangeListeners) {
        changeListener.onAfterIdentityChange(this);
      }
    }
  }

  protected static ODatabaseDocumentInternal getDatabaseIfDefinedInternal() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  void addIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.addIdentityChangeListener(identityChangeListener);
      return;
    }

    if (newIdentityChangeListeners == null) {
      newIdentityChangeListeners = Collections.newSetFromMap(new WeakHashMap<>());
    }
    newIdentityChangeListeners.add(identityChangeListener);
  }

  void removeIdentityChangeListener(OIdentityChangeListener identityChangeListener) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.removeIdentityChangeListener(identityChangeListener);
      return;
    }

    if (newIdentityChangeListeners != null) {
      newIdentityChangeListeners.remove(identityChangeListener);
    }
  }

  protected void setup(ODatabaseDocumentInternal db) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.setup(db);
      return;
    }

    if (recordId == null) {
      recordId = new OEmptyRecordId();
    }
  }

  protected void checkForLoading() {
    if (primaryRecord != null) {
      primaryRecord.checkForLoading();
      primaryRecord = primaryRecord.getRecord();
      return;
    }

    assert loadingCounter >= 0;
    if (loadingCounter > 0) {
      return;
    }

    if (status == ORecordElement.STATUS.NOT_LOADED
        && ODatabaseRecordThreadLocal.instance().isDefined()) {
      if (!getIdentity().isValid()) {
        return;
      }

      try {
        getDatabase().reload(this, null, true, false);
      } catch (OOfflineClusterException | ORecordNotFoundException e) {
        throw e;
      } catch (Exception e) {
        throw OException.wrapException(new ORecordNotFoundException(getIdentity()), e);
      }
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
    checkForLoading();
    if (primaryRecord != null) {
      return primaryRecord.isContentChanged();
    }

    return contentChanged;
  }

  protected void setContentChanged(boolean contentChanged) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.setContentChanged(contentChanged);
      return;
    }

    this.contentChanged = contentChanged;
  }

  protected void clearSource() {
    if (primaryRecord != null) {
      primaryRecord.clearSource();
      return;
    }

    this.source = null;
  }

  protected ODirtyManager getDirtyManager() {
    if (primaryRecord != null) {
      return primaryRecord.getDirtyManager();
    }

    if (this.dirtyManager == null) {
      this.dirtyManager = new ODirtyManager();
      if (this.getIdentity().isNew() && getOwner() == null) {
        this.dirtyManager.setDirty(this);
      }
    }
    return this.dirtyManager;
  }

  void setDirtyManager(ODirtyManager dirtyManager) {
    checkForLoading();
    if (primaryRecord != null) {
      primaryRecord.setDirtyManager(dirtyManager);
      return;
    }

    if (this.dirtyManager != null && dirtyManager != null) {
      dirtyManager.merge(this.dirtyManager);
    }
    this.dirtyManager = dirtyManager;
    if (this.getIdentity().isNew() && getOwner() == null && this.dirtyManager != null) {
      this.dirtyManager.setDirty(this);
    }
  }

  protected void track(OIdentifiable id) {
    if (primaryRecord != null) {
      primaryRecord.track(id);
      return;
    }

    this.getDirtyManager().track(this, id);
  }

  protected void unTrack(OIdentifiable id) {
    if (primaryRecord != null) {
      primaryRecord = primaryRecord.getRecord();
      primaryRecord.unTrack(id);
      return;
    }

    this.getDirtyManager().unTrack(this, id);
  }
}
