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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.StatefulEdge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public final class VertexDelegate implements VertexInternal {

  private final EntityImpl entity;

  public VertexDelegate(EntityImpl entry) {
    this.entity = entry;
  }

  @Override
  public void delete() {
    entity.delete();
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public boolean isEdge() {
    return false;
  }

  @Nonnull
  @Override
  public Edge castToEdge() {
    throw new DatabaseException("Not an edge");
  }

  @Nullable
  @Override
  public Edge asEdge() {
    return null;
  }

  @Override
  @Nullable
  public StatefulEdge asStatefulEdge() {
    return null;
  }

  @Override
  public boolean isBlob() {
    return false;
  }

  @Nonnull
  @Override
  public Blob castToBlob() {
    throw new DatabaseException("Not a blob");
  }

  @Nullable
  @Override
  public Blob asBlob() {
    return null;
  }

  @Nonnull
  @Override
  public DBRecord castToRecord() {
    return entity;
  }

  @Nullable
  @Override
  public DBRecord asRecord() {
    return entity;
  }

  @Override
  public boolean isRecord() {
    return true;
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  @Override
  public boolean isVertex() {
    return true;
  }

  @Override
  public boolean isStatefulEdge() {
    return false;
  }

  @Nullable
  @Override
  public SchemaClass getSchemaClass() {
    return entity.getSchemaClass();
  }

  @Nullable
  @Override
  public String getSchemaClassName() {
    return entity.getSchemaClassName();
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  @Override
  public EntityImpl getRecord(@Nonnull DatabaseSession session) {
    return entity;
  }

  @Override
  public int compareTo(Identifiable o) {
    return entity.compareTo(o);
  }

  @Override
  public int compare(Identifiable o1, Identifiable o2) {
    return entity.compare(o1, o2);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Identifiable)) {
      return false;
    }
    if (!(obj instanceof Entity)) {
      obj = ((Identifiable) obj).getRecordSilently(entity.getSession());
    }

    if (obj == null) {
      return false;
    }

    return entity.equals(((Entity) obj).getRecordSilently(entity.getSession()));
  }

  @Override
  public int hashCode() {
    return entity.hashCode();
  }

  @Override
  public void clear() {
    entity.clear();
  }

  @Override
  public boolean isEmbedded() {
    return false;
  }

  @Nonnull
  @Override
  public RID getIdentity() {
    return entity.getIdentity();
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Nonnull
  @Override
  public Entity castToEntity() {
    return entity;
  }

  @Nullable
  @Override
  public Entity asEntity() {
    return entity;
  }

  @Override
  public int getVersion() {
    return entity.getVersion();
  }

  @Override
  public boolean isDirty() {
    return entity.isDirty();
  }

  @Override
  public void updateFromJSON(@Nonnull String iJson) {
    entity.updateFromJSON(iJson);
  }

  @Override
  public @Nonnull String toJSON() {
    return entity.toJSON();
  }

  @Nonnull
  @Override
  public String toJSON(@Nonnull String iFormat) {
    return entity.toJSON(iFormat);
  }

  @Override
  public String toString() {
    if (entity != null) {
      return entity.toString();
    }
    return super.toString();
  }

  @Nonnull
  @Override
  public EntityImpl getBaseEntity() {
    return entity;
  }

  @Override
  public void updateFromMap(@Nonnull Map<String, ?> map) {
    entity.updateFromMap(map);
  }

  @Override
  public void updateFromResult(@Nonnull Result result) {
    entity.updateFromResult(result);
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap() {
    return entity.toMap();
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap(boolean includeMetadata) {
    return entity.toMap(includeMetadata);
  }

  @Override
  public DatabaseSession getBoundedToSession() {
    return entity.getBoundedToSession();
  }

  @Nonnull
  @Override
  public Result detach() {
    return entity.detach();
  }

  @Nullable
  @Override
  public SchemaImmutableClass getImmutableSchemaClass(@Nonnull DatabaseSessionInternal session) {
    return entity.getImmutableSchemaClass(session);
  }
}
