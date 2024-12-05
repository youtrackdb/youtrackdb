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

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class VertexDelegate implements VertexInternal {

  protected final EntityImpl element;

  public VertexDelegate(EntityImpl entry) {
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
  public Optional<Vertex> asVertex() {
    return Optional.of(this);
  }

  @Nonnull
  @Override
  public Vertex toVertex() {
    return this;
  }

  @Override
  public Optional<Edge> asEdge() {
    return Optional.empty();
  }

  @Nullable
  @Override
  public Edge toEdge() {
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
  public Optional<YTClass> getSchemaType() {
    return element.getSchemaType();
  }

  @Nullable
  @Override
  public YTClass getSchemaClass() {
    return element.getSchemaClass();
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  @Override
  public EntityImpl getRecord() {
    return element;
  }

  @Override
  public int compareTo(YTIdentifiable o) {
    return element.compareTo(o);
  }

  @Override
  public int compare(YTIdentifiable o1, YTIdentifiable o2) {
    return element.compare(o1, o2);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof YTIdentifiable)) {
      return false;
    }
    if (!(obj instanceof Entity)) {
      obj = ((YTIdentifiable) obj).getRecordSilently();
    }

    if (obj == null) {
      return false;
    }

    return element.equals(((Entity) obj).getRecordSilently());
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
  public VertexDelegate copy() {
    return new VertexDelegate(element.copy());
  }

  @Override
  public boolean isEmbedded() {
    return false;
  }

  @Override
  public YTRID getIdentity() {
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
  public EntityImpl getBaseDocument() {
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
