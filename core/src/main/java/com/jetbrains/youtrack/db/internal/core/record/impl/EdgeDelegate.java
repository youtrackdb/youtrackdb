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
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class EdgeDelegate implements EdgeInternal {

  protected Vertex vOut;
  protected Vertex vIn;
  protected SchemaImmutableClass lightweightEdgeType;
  protected String lightwightEdgeLabel;

  protected EntityImpl element;

  public EdgeDelegate(
      Vertex out, Vertex in, SchemaImmutableClass lightweightEdgeType, String edgeLabel) {
    vOut = out;
    vIn = in;
    this.lightweightEdgeType = lightweightEdgeType;
    this.lightwightEdgeLabel = edgeLabel;
  }

  public EdgeDelegate(EntityImpl elem) {
    this.element = elem;
  }

  @Override
  public Vertex getFrom() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    final EntityImpl entity;
    try {
      entity = getRecord();
    } catch (RecordNotFoundException rnf) {
      return null;
    }

    Object result = entity.getProperty(DIRECTION_OUT);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null; // TODO optional...?
    }
    return v.toVertex();
  }

  @Override
  public Identifiable getFromIdentifiable() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    final EntityImpl entity;
    try {
      entity = getRecord();
    } catch (RecordNotFoundException rnf) {
      return null;
    }

    var result = entity.getLinkProperty(DIRECTION_OUT);
    assert result != null;

    var id = result.getIdentity();
    var db = entity.getSession();
    var schema = db.getMetadata().getSchema();

    if (schema.getClassByClusterId(id.getClusterId()).isVertexType()) {
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

    final EntityImpl entity;
    try {
      entity = getRecord();
    } catch (RecordNotFoundException rnf) {
      return null;
    }

    Object result = entity.getProperty(DIRECTION_IN);
    if (!(result instanceof Entity v)) {
      return null;
    }
    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  public Identifiable getToIdentifiable() {
    if (vIn != null) {
      // LIGHTWEIGHT EDGE
      return vIn;
    }

    final EntityImpl entity;

    try {
      entity = getRecord();
    } catch (RecordNotFoundException rnf) {
      return null;
    }

    var result = entity.getLinkProperty(DIRECTION_IN);
    assert result != null;

    var id = result.getIdentity();
    var schema = entity.getSession().getMetadata().getSchema();

    if (schema.getClassByClusterId(id.getClusterId()).isVertexType()) {
      return id;
    }

    return null;
  }

  @Override
  public boolean isLightweight() {
    return this.element == null;
  }

  public void delete() {
    if (element != null) {
      element.delete();
    } else {
      EdgeEntityImpl.deleteLinks(this);
    }
  }

  @Override
  public void promoteToRegularEdge() {

    var from = getFrom();
    Vertex to = getTo();
    VertexInternal.removeOutgoingEdge(from, this);
    VertexInternal.removeIncomingEdge(to, this);

    var db = element.getSession();
    this.element =
        db.newRegularEdge(
                lightweightEdgeType == null ? "E" : lightweightEdgeType.getName(), from, to)
            .getRecord();
    this.lightweightEdgeType = null;
    this.vOut = null;
    this.vIn = null;
  }

  @Override
  @Nullable
  public EntityImpl getBaseDocument() {
    return element;
  }

  @Override
  public Optional<Vertex> asVertex() {
    return Optional.empty();
  }

  @Nullable
  @Override
  public Vertex toVertex() {
    return null;
  }

  @Override
  public Optional<Edge> asEdge() {
    return Optional.of(this);
  }

  @Nonnull
  @Override
  public Edge toEdge() {
    return this;
  }

  @Override
  public boolean isVertex() {
    return false;
  }

  @Override
  public boolean isEdge() {
    return true;
  }

  @Override
  public Optional<SchemaClass> getSchemaType() {
    if (element == null) {
      return Optional.ofNullable(lightweightEdgeType);
    }
    return element.getSchemaType();
  }

  @Nullable
  @Override
  public SchemaClass getSchemaClass() {
    if (element == null) {
      return lightweightEdgeType;
    }
    return element.getSchemaClass();
  }

  public boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }
    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    Optional<SchemaClass> typeClass = getSchemaType();
    if (typeClass.isPresent()) {
      types.add(typeClass.get().getName());
      typeClass.get().getAllSuperClasses().stream()
          .map(x -> x.getName())
          .forEach(name -> types.add(name));
    } else {
      if (lightwightEdgeLabel != null) {
        types.add(lightwightEdgeLabel);
      } else {
        types.add("E");
      }
    }
    for (String s : labels) {
      for (String type : types) {
        if (type.equalsIgnoreCase(s)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public RID getIdentity() {
    if (element == null) {
      return null;
    }
    return element.getIdentity();
  }

  @Nonnull
  @Override
  public <T extends DBRecord> T getRecord() {

    if (element == null) {
      return null;
    }
    return (T) element;
  }

  @Override
  public int compare(Identifiable o1, Identifiable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compareTo(Identifiable o) {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (element == null) {
      return this == obj;
      // TODO double-check this logic for lightweight edges
    }
    if (!(obj instanceof Identifiable)) {
      return false;
    }
    if (!(obj instanceof Entity)) {
      obj = ((Identifiable) obj).getRecord();
    }

    return element.equals(((Entity) obj).getRecord());
  }

  @Override
  public int hashCode() {
    if (element == null) {
      return super.hashCode();
    }

    return element.hashCode();
  }

  @Override
  public void clear() {
    if (element != null) {
      element.clear();
    }
  }

  @Override
  public int getVersion() {
    if (element != null) {
      return element.getVersion();
    }
    return 1;
  }

  @Override
  public boolean isDirty() {
    if (element != null) {
      return element.isDirty();
    }
    return false;
  }

  @Override
  public void save() {
    if (element != null) {
      element.save();
    } else {
      vIn.save();
    }
  }

  @Override
  public void fromJSON(String iJson) {
    if (element == null) {
      promoteToRegularEdge();
    }
    element.fromJSON(iJson);
  }

  @Override
  public String toJSON() {
    if (element != null) {
      return element.toJSON();
    } else {
      return "{\"out\":\""
          + vOut.getIdentity()
          + "\", \"in\":\""
          + vIn.getIdentity()
          + "\", \"@class\":\""
          + StringSerializerHelper.encode(lightweightEdgeType.getName())
          + "\"}";
    }
  }

  @Override
  public String toJSON(String iFormat) {
    if (element != null) {
      return element.toJSON(iFormat);
    } else {
      return "{\"out\":\""
          + vOut.getIdentity()
          + "\", \"in\":\""
          + vIn.getIdentity()
          + "\", \"@class\":\""
          + StringSerializerHelper.encode(lightweightEdgeType.getName())
          + "\"}";
    }
  }

  @Override
  public void fromMap(Map<String, ?> map) {
    if (element != null) {
      element.fromMap(map);
    }

    throw new UnsupportedOperationException("fromMap is not supported for lightweight edges");
  }

  @Override
  public Map<String, Object> toMap() {
    return Map.of(DIRECTION_OUT, getToIdentifiable(), DIRECTION_IN, getFromIdentifiable());
  }

  @Override
  public boolean isNotBound(DatabaseSession session) {
    if (element != null) {
      return element.isNotBound(session);
    }

    return false;
  }

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    } else {
      StringBuilder result = new StringBuilder();
      boolean first = true;
      result.append("{");
      if (lightweightEdgeType != null) {
        result.append("class: " + lightweightEdgeType.getName());
        first = false;
      }
      if (vOut != null) {
        if (!first) {
          result.append(", ");
        }
        result.append("out: " + vOut.getIdentity());
        first = false;
      }
      if (vIn != null) {
        if (!first) {
          result.append(", ");
        }
        result.append("in: " + vIn.getIdentity());
        first = false;
      }
      result.append("} (lightweight)");
      return result.toString();
    }
  }
}
