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
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EdgeDelegate implements EdgeInternal, StatefulEdge {

  protected Vertex vOut;
  protected Vertex vIn;
  protected SchemaImmutableClass lightweightEdgeType;
  protected String lightwightEdgeLabel;

  protected EntityImpl entity;
  protected final DatabaseSessionInternal session;

  public EdgeDelegate(DatabaseSessionInternal session,
      Vertex out, Vertex in,
      SchemaImmutableClass lightweightEdgeType,
      String edgeLabel) {
    vOut = out;
    vIn = in;
    this.lightweightEdgeType = lightweightEdgeType;
    this.lightwightEdgeLabel = edgeLabel;
    this.session = session;
  }

  public EdgeDelegate(EntityImpl elem) {
    this.entity = elem;
    this.session = elem.getSession();
  }

  @Override
  public Vertex getFrom() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    if (entity == null) {
      return null;
    }

    var result = entity.getProperty(DIRECTION_OUT);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }
    return v.castToVertex();
  }

  @Override
  public Identifiable getFromLink() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    if (entity == null) {
      return null;
    }

    var result = entity.getLinkProperty(DIRECTION_OUT);
    assert result != null;

    var id = result.getIdentity();
    var schema = session.getMetadata().getSchema();

    if (schema.getClassByClusterId(id.getClusterId()).isVertexType(session)) {
      return id;
    }

    return null;
  }

  @Override
  public boolean isEmbedded() {
    return false;
  }

  @Override
  public Vertex getTo() {
    if (vIn != null)
    // LIGHTWEIGHT EDGE
    {
      return vIn;
    }

    if (entity == null) {
      return null;
    }

    var result = entity.getProperty(DIRECTION_IN);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.castToVertex();
  }

  @Override
  public Identifiable getToLink() {
    if (vIn != null) {
      // LIGHTWEIGHT EDGE
      return vIn;
    }

    if (entity == null) {
      return null;
    }

    var result = entity.getLinkProperty(DIRECTION_IN);
    assert result != null;

    var id = result.getIdentity();
    var schema = entity.getSession().getMetadata().getSchema();

    if (schema.getClassByClusterId(id.getClusterId()).isVertexType(session)) {
      return id;
    }

    return null;
  }

  @Override
  public boolean isLightweight() {
    return this.entity == null;
  }

  public void delete() {
    if (entity != null) {
      entity.delete();
    } else {
      EdgeEntityImpl.deleteLinks(session, this);
    }
  }


  @Nullable
  @Override
  public Identifiable getLinkProperty(@Nonnull String fieldName) {
    checkStateful();

    EdgeInternal.checkPropertyName(fieldName);
    return entity.getLinkProperty(fieldName);
  }

  private void checkStateful() {
    if (entity == null) {
      throw new DatabaseException(
          "This is a lightweight edge instance and does not keep any state inside.");
    }
  }

  @Override
  public @Nonnull Collection<String> getPropertyNames() {
    return EdgeInternal.filterPropertyNames(getPropertyNamesInternal());
  }

  @Override
  public <RET> RET getProperty(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getProperty(name);
  }

  @Nullable
  @Override
  public Entity getEntityProperty(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getEntityProperty(name);
  }

  @Nullable
  @Override
  public Blob getBlobProperty(String propertyName) {
    checkStateful();
    EdgeInternal.checkPropertyName(propertyName);

    return entity.getBlobProperty(propertyName);
  }


  @Override
  public boolean isUnloaded() {
    checkStateful();

    return entity.isUnloaded();
  }

  @Override
  public boolean exists() {
    checkStateful();

    return entity.exists();
  }

  @Override
  public void setProperty(@Nonnull String name, @Nullable Object value) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    entity.setProperty(name, value);
  }

  @Override
  public @Nonnull <T> List<T> getOrCreateEmbeddedList(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateEmbeddedList(name);
  }

  @Override
  public @Nonnull <T> Set<T> getOrCreateEmbeddedSet(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateEmbeddedSet(name);
  }

  @Override
  public @Nonnull <T> Map<String, T> getOrCreateEmbeddedMap(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateEmbeddedMap(name);
  }

  @Override
  public @Nonnull List<Identifiable> getOrCreateLinkList(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateLinkList(name);
  }

  @Nonnull
  @Override
  public Set<Identifiable> getOrCreateLinkSet(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateLinkSet(name);
  }

  @Nonnull
  @Override
  public Map<String, Identifiable> getOrCreateLinkMap(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.getOrCreateLinkMap(name);
  }

  @Override
  public boolean hasProperty(final @Nonnull String propertyName) {
    checkStateful();
    EdgeInternal.checkPropertyName(propertyName);

    return entity.hasProperty(propertyName);
  }

  @Override
  public void setProperty(@Nonnull String name, Object value, @Nonnull PropertyType fieldType) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    entity.setProperty(name, value, fieldType);
  }

  @Override
  public <RET> RET removeProperty(@Nonnull String name) {
    checkStateful();
    EdgeInternal.checkPropertyName(name);

    return entity.removeProperty(name);
  }


  @Override
  public Collection<String> getPropertyNamesInternal() {
    checkStateful();

    return entity.getPropertyNamesInternal();
  }

  public <RET> RET getPropertyInternal(String name) {
    checkStateful();

    return entity.getPropertyInternal(name);
  }

  @Override
  public <RET> RET getPropertyInternal(String name, boolean lazyLoading) {
    checkStateful();

    return entity.getPropertyInternal(name, lazyLoading);
  }

  @Override
  public <RET> RET getPropertyOnLoadValue(@Nonnull String name) {
    checkStateful();

    return entity.getPropertyOnLoadValue(name);
  }

  @Nullable
  @Override
  public Identifiable getLinkPropertyInternal(String name) {
    checkStateful();

    return entity.getLinkPropertyInternal(name);
  }

  @Override
  public void setPropertyInternal(String name, Object value) {
    checkStateful();

    entity.setPropertyInternal(name, value);
  }

  @Override
  public void setPropertyInternal(String name, Object value, PropertyType type) {
    checkStateful();

    entity.setPropertyInternal(name, value, type);
  }

  @Override
  public <RET> RET removePropertyInternal(String name) {
    checkStateful();

    return entity.removePropertyInternal(name);
  }

  @Override
  public boolean isVertex() {
    return false;
  }

  @Override
  public boolean isEdge() {
    return true;
  }

  @Nonnull
  @Override
  public Edge castToEdge() {
    return this;
  }

  @Nullable
  @Override
  public Edge asEdge() {
    return this;
  }

  @Override
  public boolean isStatefulEdge() {
    return true;
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

  @Nonnull
  @Override
  public DBRecord castToRecord() {
    return entity;
  }

  @Override
  public boolean isRecord() {
    return entity != null;
  }

  @Override
  public boolean isProjection() {
    return false;
  }


  @Nonnull
  @Override
  public SchemaClass getSchemaClass() {
    if (entity == null) {
      return lightweightEdgeType;
    }
    return entity.getSchemaClass();
  }

  @Nonnull
  @Override
  public String getSchemaClassName() {
    if (entity == null) {
      return lightweightEdgeType.getName(session);
    }

    return entity.getSchemaClassName();
  }

  public boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }
    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    var typeClass = getSchemaClass();
    types.add(typeClass.getName(session));
    typeClass.getAllSuperClasses().stream()
        .map(x -> x.getName(session))
        .forEach(types::add);
    for (var s : labels) {
      for (var type : types) {
        if (type.equalsIgnoreCase(s)) {
          return true;
        }
      }
    }

    return false;
  }

  @Nonnull
  @Override
  public RID getIdentity() {
    if (entity == null) {
      return null;
    }

    return entity.getIdentity();
  }

  @Override
  public boolean isEntity() {
    return entity != null;
  }

  @Nonnull
  @Override
  public Entity castToEntity() {
    return entity;
  }

  @Nonnull
  @Override
  public <T extends DBRecord> T getRecord(@Nonnull DatabaseSession session) {
    if (entity == null) {
      return null;
    }

    //noinspection unchecked
    return (T) entity;
  }

  @Override
  public int compare(Identifiable o1, Identifiable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compareTo(@Nonnull Identifiable o) {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (entity == null) {
      return this == obj;
      // TODO double-check this logic for lightweight edges
    }

    if (!(obj instanceof Identifiable)) {
      return false;
    }

    var session = entity.getSession();
    if (!(obj instanceof Entity)) {
      obj = ((Identifiable) obj).getRecord(session);
    }

    return entity.equals(((Entity) obj).getRecord(session));
  }

  @Override
  public int hashCode() {
    if (entity == null) {
      return super.hashCode();
    }

    return entity.hashCode();
  }

  @Override
  public void clear() {
    if (entity != null) {
      entity.clear();
    }
  }

  @Override
  public int getVersion() {
    if (entity != null) {
      return entity.getVersion();
    }
    return 1;
  }

  @Override
  public boolean isDirty() {
    if (entity != null) {
      return entity.isDirty();
    }
    return false;
  }

  @Override
  public void save() {
    if (entity != null) {
      entity.save();
    } else {
      vIn.save();
    }
  }

  @Override
  public void updateFromJSON(@Nonnull String iJson) {
    if (entity == null) {
      throw new UnsupportedOperationException("fromJSON is not supported for lightweight edges");
    }

    entity.updateFromJSON(iJson);
  }

  @Override
  public @Nonnull String toJSON() {
    if (entity != null) {
      return entity.toJSON();
    } else {
      return "{\"out\":\""
          + vOut.getIdentity()
          + "\", \"in\":\""
          + vIn.getIdentity()
          + "\", \"@class\":\""
          + StringSerializerHelper.encode(lightweightEdgeType.getName(session))
          + "\"}";
    }
  }

  @Nonnull
  @Override
  public String toJSON(@Nonnull String iFormat) {
    if (entity != null) {
      return entity.toJSON(iFormat);
    } else {
      return "{\"out\":\""
          + vOut.getIdentity()
          + "\", \"in\":\""
          + vIn.getIdentity()
          + "\", \"@class\":\""
          + StringSerializerHelper.encode(lightweightEdgeType.getName(session))
          + "\"}";
    }
  }

  @Override
  public void updateFromMap(@Nonnull Map<String, ?> map) {
    if (entity != null) {
      entity.updateFromMap(map);
    }

    throw new UnsupportedOperationException("fromMap is not supported for lightweight edges");
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap() {
    if (entity != null) {
      return entity.toMap();
    }

    return Map.of(DIRECTION_OUT, getToLink(), DIRECTION_IN, getFromLink());
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap(boolean includeMetadata) {
    if (entity != null) {
      return entity.toMap(includeMetadata);
    }

    return Map.of(DIRECTION_OUT, getToLink(), DIRECTION_IN, getFromLink());
  }

  @Override
  public boolean isNotBound(@Nonnull DatabaseSession session) {
    if (entity != null) {
      return entity.isNotBound(session);
    }

    return this.session != session;
  }

  @Nullable
  @Override
  public DatabaseSession getBoundedToSession() {
    return session;
  }

  @Override
  public @Nonnull Result detach() {
    if (entity != null) {
      return entity.detach();
    }

    var result = new ResultInternal(null);
    result.setProperty(DIRECTION_OUT, vOut.getIdentity());
    result.setProperty(DIRECTION_IN, vIn.getIdentity());
    return result;
  }

  @Nullable
  @Override
  public Entity asEntity() {
    return entity;
  }

  @Nullable
  @Override
  public Blob asBlob() {
    throw new DatabaseException("Not a blob");
  }

  @Nullable
  @Override
  public DBRecord asRecord() {
    return entity;
  }

  @Nonnull
  @Override
  public StatefulEdge castToStatefulEdge() {
    if (entity != null) {
      return this;
    }

    throw new DatabaseException("Current edge instance is not a stateful edge");
  }

  @Nullable
  @Override
  public StatefulEdge asStatefulEdge() {
    if (entity != null) {
      return this;
    }

    return null;
  }
}
