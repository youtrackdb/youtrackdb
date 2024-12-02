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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class OVertexDelegate implements OVertexInternal {

  protected final ODocument element;

  public OVertexDelegate(ODocument entry) {
    this.element = entry;
  }

  @Override
  public void delete() {
    element.delete();
  }

  public void resetToNew() {
    element.resetToNew();
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

  @Nonnull
  @SuppressWarnings("unchecked")
  @Override
  public ODocument getRecord() {
    return element;
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
      obj = ((OIdentifiable) obj).getRecordSilently();
    }

    if (obj == null) {
      return false;
    }

    return element.equals(((OElement) obj).getRecordSilently());
  }

  @Override
  public int hashCode() {
    return element.hashCode();
  }

  @Override
  public void clear() {
    element.clear();
  }

  @Override
  public OVertexDelegate copy() {
    return new OVertexDelegate(element.copy());
  }

  @Override
  public boolean isEmbedded() {
    return false;
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
  public boolean isDirty() {
    return element.isDirty();
  }

  @Override
  public void save() {
    element.save();
  }

  @Override
  public void fromJSON(String iJson) {
    element.fromJSON(iJson);
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

  @Override
  public void fromMap(Map<String, ?> map) {
    element.fromMap(map);
  }

  @Override
  public Map<String, Object> toMap() {
    return element.toMap();
  }
}
