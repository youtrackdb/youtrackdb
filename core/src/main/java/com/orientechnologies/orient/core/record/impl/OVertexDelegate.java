/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Luigi Dell'Aquila
 */
public class OVertexDelegate implements OVertexInternal {

  protected final ODocument element;

  public OVertexDelegate(ODocument entry) {
    this.element = entry;
  }

  @Override
  public OVertex delete() {
    element.delete();
    return this;
  }

  @Override
  public boolean isProxy() {
    return true;
  }

  @Override
  public Optional<OVertex> asVertex() {
    return Optional.of(this);
  }

  @Nonnull
  @Override
  public OVertex toVertex() {
    return this;
  }

  @Override
  public Optional<OEdge> asEdge() {
    return Optional.empty();
  }

  @Nullable
  @Override
  public OEdge toEdge() {
    return null;
  }

  @Override
  public boolean isVertex() {
    return true;
  }

  @Override
  public boolean isEdge() {
    return false;
  }

  @Override
  public Optional<OClass> getSchemaType() {
    return element.getSchemaType();
  }

  @Nullable
  @Override
  public OClass getSchemaClass() {
    return element.getSchemaClass();
  }

  @SuppressWarnings("unchecked")
  @Override
  public ODocument getRecord() {
    return element;
  }

  @Override
  public void lock(boolean iExclusive) {
    element.lock(iExclusive);
  }

  @Override
  public boolean isLocked() {
    return element.isLocked();
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return element.lockingStrategy();
  }

  @Override
  public void unlock() {
    element.unlock();
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return element.compareTo(o);
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return element.compare(o1, o2);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OIdentifiable)) {
      return false;
    }
    if (!(obj instanceof OElement)) {
      obj = ((OIdentifiable) obj).getRecord();
    }

    return element.equals(((OElement) obj).getRecord());
  }

  @Override
  public int hashCode() {
    return element.hashCode();
  }

  @Override
  public STATUS getInternalStatus() {
    return element.getInternalStatus();
  }

  @Override
  public void setInternalStatus(STATUS iStatus) {
    element.setInternalStatus(iStatus);
  }

  @SuppressWarnings("unchecked")
  @Override
  public OVertexDelegate setDirty() {
    element.setDirty();
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    element.setDirtyNoChanged();
  }

  @Override
  public ORecordElement getOwner() {
    return element.getOwner();
  }

  @Override
  public byte[] toStream() throws OSerializationException {
    return element.toStream();
  }

  @Override
  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    return element.fromStream(iStream);
  }

  @Override
  public boolean detach() {
    return element.detach();
  }

  @Override
  public <RET extends ORecord> RET unload() {
    element.unload();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET clear() {
    element.clear();
    return (RET) this;
  }

  @Override
  public OVertexDelegate copy() {
    return new OVertexDelegate(element.copy());
  }

  @Override
  public ORID getIdentity() {
    return element.getIdentity();
  }

  @Override
  public int getVersion() {
    return element.getVersion();
  }

  @Override
  public ODatabaseDocument getDatabase() {
    return element.getDatabase();
  }

  @Override
  public boolean isDirty() {
    return element.isDirty();
  }

  @Override
  public <RET extends ORecord> RET load() throws ORecordNotFoundException {
    return (RET) element.load();
  }

  @Override
  public <RET extends ORecord> RET reload() throws ORecordNotFoundException {
    element.reload();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET reload(String fetchPlan, boolean ignoreCache, boolean force)
      throws ORecordNotFoundException {
    element.reload(fetchPlan, ignoreCache, force);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save() {
    element.save();
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET save(String iCluster) {
    element.save(iCluster);
    return (RET) this;
  }

  @Override
  public <RET extends ORecord> RET fromJSON(String iJson) {
    element.fromJSON(iJson);
    return (RET) this;
  }

  @Override
  public String toJSON() {
    return element.toJSON();
  }

  @Override
  public String toJSON(String iFormat) {
    return element.toJSON(iFormat);
  }

  @Override
  public int getSize() {
    return element.getSize();
  }

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    }
    return super.toString();
  }

  @Nonnull
  @Override
  public ODocument getBaseDocument() {
    return element;
  }
}
