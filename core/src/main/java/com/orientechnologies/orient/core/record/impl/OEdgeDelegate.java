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

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class OEdgeDelegate implements OEdgeInternal {

  protected OVertex vOut;
  protected OVertex vIn;
  protected OImmutableClass lightweightEdgeType;
  protected String lightwightEdgeLabel;

  protected ODocument element;

  public OEdgeDelegate(
      OVertex out, OVertex in, OImmutableClass lightweightEdgeType, String edgeLabel) {
    vOut = out;
    vIn = in;
    this.lightweightEdgeType = lightweightEdgeType;
    this.lightwightEdgeLabel = edgeLabel;
  }

  public OEdgeDelegate(ODocument elem) {
    this.element = elem;
  }

  @Override
  public OVertex getFrom() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    final ODocument doc;
    try {
      doc = getRecord();
    } catch (ORecordNotFoundException rnf) {
      return null;
    }

    Object result = doc.getProperty(DIRECTION_OUT);
    if (!(result instanceof OElement v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null; // TODO optional...?
    }
    return v.toVertex();
  }

  @Override
  public OIdentifiable getFromIdentifiable() {
    if (vOut != null) {
      // LIGHTWEIGHT EDGE
      return vOut;
    }

    final ODocument doc;
    try {
      doc = getRecord();
    } catch (ORecordNotFoundException rnf) {
      return null;
    }

    var result = doc.getLinkProperty(DIRECTION_OUT);
    assert result != null;

    var id = result.getIdentity();
    var db = doc.getSession();
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
  public OVertex getTo() {
    if (vIn != null)
    // LIGHTWEIGHT EDGE
    {
      return vIn;
    }

    final ODocument doc;
    try {
      doc = getRecord();
    } catch (ORecordNotFoundException rnf) {
      return null;
    }

    Object result = doc.getProperty(DIRECTION_IN);
    if (!(result instanceof OElement v)) {
      return null;
    }
    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  public OIdentifiable getToIdentifiable() {
    if (vIn != null) {
      // LIGHTWEIGHT EDGE
      return vIn;
    }

    final ODocument doc;

    try {
      doc = getRecord();
    } catch (ORecordNotFoundException rnf) {
      return null;
    }

    var result = doc.getLinkProperty(DIRECTION_IN);
    assert result != null;

    var id = result.getIdentity();
    var schema = doc.getSession().getMetadata().getSchema();

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
      OEdgeDocument.deleteLinks(this);
    }
  }

  @Override
  public void promoteToRegularEdge() {

    var from = getFrom();
    OVertex to = getTo();
    OVertexInternal.removeOutgoingEdge(from, this);
    OVertexInternal.removeIncomingEdge(to, this);

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
  public ODocument getBaseDocument() {
    return element;
  }

  @Override
  public Optional<OVertex> asVertex() {
    return Optional.empty();
  }

  @Nullable
  @Override
  public OVertex toVertex() {
    return null;
  }

  @Override
  public Optional<OEdge> asEdge() {
    return Optional.of(this);
  }

  @Nonnull
  @Override
  public OEdge toEdge() {
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
  public Optional<OClass> getSchemaType() {
    if (element == null) {
      return Optional.ofNullable(lightweightEdgeType);
    }
    return element.getSchemaType();
  }

  @Nullable
  @Override
  public OClass getSchemaClass() {
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

    Optional<OClass> typeClass = getSchemaType();
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
  public ORID getIdentity() {
    if (element == null) {
      return null;
    }
    return element.getIdentity();
  }

  @Nonnull
  @Override
  public <T extends ORecord> T getRecord() {

    if (element == null) {
      return null;
    }
    return (T) element;
  }

  @Override
  public int compare(OIdentifiable o1, OIdentifiable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compareTo(OIdentifiable o) {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (element == null) {
      return this == obj;
      // TODO double-check this logic for lightweight edges
    }
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
          + OStringSerializerHelper.encode(lightweightEdgeType.getName())
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
          + OStringSerializerHelper.encode(lightweightEdgeType.getName())
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
  public boolean isNotBound(ODatabaseSession session) {
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
