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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableIdentity;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.IdentityChangeListener;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.SerializableStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJackson;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@SuppressWarnings({"unchecked"})
public abstract class RecordAbstract implements DBRecord, RecordElement, SerializableStream,
    ChangeableIdentity {

  public static final String BASE_FORMAT =
      "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded";
  private static final String DEFAULT_FORMAT = BASE_FORMAT + "," + "fetchPlan:*:0";
  public static final String OLD_FORMAT_WITH_LATE_TYPES = BASE_FORMAT + "," + "fetchPlan:*:0";

  protected final RecordId recordId;
  protected int recordVersion = 0;

  protected byte[] source;
  protected int size;

  public RecordSerializer recordFormat;
  protected long dirty = 1;
  protected boolean contentChanged = true;
  protected STATUS status = STATUS.LOADED;

  private long loadingCounter;
  protected DatabaseSessionInternal session;

  public RecordAbstract(DatabaseSessionInternal db) {
    recordId = new ChangeableRecordId();
    this.session = db;
  }

  public RecordAbstract(DatabaseSessionInternal db, final byte[] iSource) {
    source = iSource;
    size = iSource.length;
    recordId = new ChangeableRecordId();

    unsetDirty();
    session = db;
  }

  public long getDirtyCounter() {
    return dirty;
  }

  @Nonnull
  public final RecordId getIdentity() {
    return recordId;
  }

  @Override
  public RecordElement getOwner() {
    return null;
  }

  @Nonnull
  public RecordAbstract getRecord(DatabaseSession db) {
    return this;
  }

  public void clear() {
    checkForBinding();

    setDirty();
  }

  /**
   * Resets the record to be reused. The record is fresh like just created.
   */
  public RecordAbstract reset() {
    status = STATUS.LOADED;
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
      source = recordFormat.toStream(session, this);
    }

    return source;
  }

  public RecordAbstract fromStream(final byte[] iRecordBuffer) {
    var session = getSession();
    if (dirty > 0) {
      throw new DatabaseException(session.getDatabaseName(),
          "Cannot call fromStream() on dirty records");
    }

    contentChanged = false;
    source = iRecordBuffer;
    size = iRecordBuffer != null ? iRecordBuffer.length : 0;
    status = STATUS.LOADED;

    return this;
  }


  @Override
  public boolean isEmbedded() {
    return false;
  }

  public RecordAbstract setDirty() {
    dirty++;
    if (dirty == 1 && status != STATUS.UNMARSHALLING) {
      checkForBinding();
      registerInTx();
      source = null;
    }

    contentChanged = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (dirty == 0 && status != STATUS.UNMARSHALLING) {
      checkForBinding();
      registerInTx();

      source = null;
    }

    dirty++;
  }

  private void registerInTx() {
    if (recordId.isPersistent()) {
      if (session == null) {
        throw new DatabaseException(createNotBoundToSessionMessage());
      }

      var tx = session.getTransaction();
      if (!tx.isActive()) {
        throw new DatabaseException(session.getDatabaseName(),
            "Cannot modify persisted record outside of transaction");
      }

      if (!isEmbedded()) {
        assert recordId.isPersistent();

        var optimistic = (FrontendTransactionOptimistic) tx;
        optimistic.addRecordOperation(this, RecordOperation.UPDATED, null);
      }
    }
  }

  public final boolean isDirty() {
    return dirty != 0;
  }


  public <RET extends DBRecord> RET updateFromJSON(final String iSource, final String iOptions) {
    status = STATUS.UNMARSHALLING;
    try {
      RecordSerializerJackson.INSTANCE.fromString(getSessionIfDefined(),
          iSource, this, null);
      // nothing change
      return (RET) this;
    } finally {
      status = STATUS.LOADED;
    }
  }

  public void updateFromJSON(final @Nonnull String iSource) {
    status = STATUS.UNMARSHALLING;
    try {
      RecordSerializerJackson.INSTANCE.fromString(getSessionIfDefined(), iSource, this, null);
    } finally {
      status = STATUS.LOADED;
    }
  }

  // Add New API to load record if rid exist
  public final <RET extends DBRecord> RET updateFromJSON(final String iSource, boolean needReload) {
    status = STATUS.UNMARSHALLING;
    try {
      return RecordSerializerJackson.INSTANCE.fromString(getSession(), iSource, this, null);
    } finally {
      status = STATUS.LOADED;
    }
  }

  public final <RET extends DBRecord> RET updateFromJSON(final InputStream iContentResult)
      throws IOException {
    status = STATUS.UNMARSHALLING;
    try {
      final var out = new ByteArrayOutputStream();
      IOUtils.copyStream(iContentResult, out);
      RecordSerializerJackson.INSTANCE.fromString(getSession(), out.toString(), this, null);
      return (RET) this;
    } finally {
      status = STATUS.LOADED;
    }
  }

  public String toJSON() {
    checkForBinding();
    return toJSON(DEFAULT_FORMAT);
  }

  public String toJSON(final String format) {
    checkForBinding();

    return RecordSerializerJackson.INSTANCE
        .toString(getSessionIfDefined(), this, new StringWriter(1024),
            format == null ? "" : format)
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

  public final void setVersion(final int iVersion) {
    recordVersion = iVersion;
  }

  public void unload() {
    if (status != STATUS.NOT_LOADED) {
      source = null;
      status = STATUS.NOT_LOADED;
      session = null;
      unsetDirty();
    }
  }

  @Override
  public boolean isUnloaded() {
    return status == STATUS.NOT_LOADED;
  }

  @Override
  public boolean isNotBound(DatabaseSession session) {
    return isUnloaded() || this.session != session;
  }

  @Nonnull
  public DatabaseSessionInternal getSession() {
    if (session == null) {
      throw new DatabaseException(createNotBoundToSessionMessage());
    }

    assert session.assertIfNotActive();
    return session;
  }

  @Nullable
  protected DatabaseSessionInternal getSessionIfDefined() {
    assert session == null || session.assertIfNotActive();
    return session;
  }

  public void save() {
    getSession().save(this, null);
  }

  public void save(final String iClusterName) {
    getSession().save(this, iClusterName);
  }

  public void delete() {
    dirty++;
    getSession().delete(this);
  }

  public int getSize() {
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

    if (obj instanceof Identifiable) {
      return recordId.equals(((Identifiable) obj).getIdentity());
    }

    return false;
  }

  public int compare(final Identifiable iFirst, final Identifiable iSecond) {
    if (iFirst == null || iSecond == null) {
      return -1;
    }

    return iFirst.compareTo(iSecond);
  }

  public int compareTo(@Nonnull final Identifiable iOther) {
    if (recordId == null) {
      return iOther.getIdentity() == null ? 0 : 1;
    }

    return recordId.compareTo(iOther.getIdentity());
  }

  public STATUS getInternalStatus() {
    return status;
  }

  @Override
  public boolean exists() {
    return getSession().exists(recordId);
  }

  public void setInternalStatus(final STATUS iStatus) {
    this.status = iStatus;
  }


  protected RecordAbstract fill(
      final RID iRid, final int iVersion, final byte[] iBuffer, boolean iDirty) {
    var session = getSession();

    if (dirty > 0) {
      throw new DatabaseException(session.getDatabaseName(), "Cannot call fill() on dirty records");
    }

    recordId.setClusterId(iRid.getClusterId());
    recordId.setClusterPosition(iRid.getClusterPosition());

    recordVersion = iVersion;
    status = STATUS.LOADED;
    source = iBuffer;
    size = iBuffer != null ? iBuffer.length : 0;

    if (source != null && source.length > 0) {
      dirty = iDirty ? 1 : 0;
      contentChanged = iDirty;
    }

    return this;
  }

  public final RecordAbstract setIdentity(final int iClusterId, final long iClusterPosition) {
    recordId.setClusterId(iClusterId);
    recordId.setClusterPosition(iClusterPosition);

    return this;
  }

  public final RecordAbstract setIdentity(RID recordId) {
    this.recordId.setClusterId(recordId.getClusterId());
    this.recordId.setClusterPosition(recordId.getClusterPosition());

    return this;
  }


  protected void unsetDirty() {
    contentChanged = false;
    dirty = 0;
  }

  public abstract byte getRecordType();

  protected void checkForBinding() {
    assert loadingCounter >= 0;

    if (loadingCounter > 0 || status == STATUS.UNMARSHALLING) {
      return;
    }

    if (status == STATUS.NOT_LOADED) {
      if (!recordId.isValid()) {
        return;
      }

      throw new DatabaseException(session != null ? session.getDatabaseName() : null,
          createNotBoundToSessionMessage());
    }

    if (session == null) {
      throw new DatabaseException(createNotBoundToSessionMessage());
    }

    assert session.assertIfNotActive();
  }

  private String createNotBoundToSessionMessage() {
    return "Record "
        + recordId
        + " is not bound to the current session. Please bind record to the database session"
        + " by calling : "
        + DatabaseSession.class.getSimpleName()
        + ".bindToSession(record) before using it.";
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

  public void resetToNew() {
    if (!recordId.isNew()) {
      throw new IllegalStateException(
          "Record id is not new " + recordId + " as expected, so record can't be reset.");
    }

    reset();
  }


  @Override
  public void addIdentityChangeListener(IdentityChangeListener identityChangeListeners) {
    if (recordId instanceof ChangeableIdentity) {
      ((ChangeableIdentity) recordId).addIdentityChangeListener(identityChangeListeners);
    }
  }

  @Override
  public void removeIdentityChangeListener(IdentityChangeListener identityChangeListener) {
    if (recordId instanceof ChangeableIdentity) {
      ((ChangeableIdentity) recordId).removeIdentityChangeListener(identityChangeListener);
    }
  }

  @Override
  public boolean canChangeIdentity() {
    if (recordId instanceof ChangeableIdentity) {
      return ((ChangeableIdentity) recordId).canChangeIdentity();
    }

    return false;
  }

  @Nullable
  @Override
  public DatabaseSession getBoundedToSession() {
    return getSessionIfDefined();
  }
}
