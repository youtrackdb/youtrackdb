package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class EdgeEntityImpl extends EntityImpl implements EdgeInternal {

  public EdgeEntityImpl(DatabaseSessionInternal session, String cl) {
    super(session, cl);
  }

  public EdgeEntityImpl(DatabaseSessionInternal session) {
    super(session);
  }


  public EdgeEntityImpl(DatabaseSessionInternal database, RecordId rid) {
    super(database, rid);
  }

  @Override
  public Vertex getFrom() {
    var result = getPropertyInternal(DIRECTION_OUT);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  @Nullable
  public Identifiable getFromIdentifiable() {
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_OUT);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType()) {
      return rid;
    }

    return null;
  }

  @Override
  public Vertex getTo() {
    var result = getPropertyInternal(DIRECTION_IN);
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
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_IN);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType()) {
      return rid;
    }

    return null;
  }

  @Override
  public boolean isLightweight() {
    // LIGHTWEIGHT EDGES MANAGED BY EdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }

  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  @Nullable
  public EntityImpl getBaseEntity() {
    return this;
  }

  @Override
  public EdgeEntityImpl copy() {
    checkForBinding();

    return (EdgeEntityImpl) super.copyTo(new EdgeEntityImpl(getSession()));
  }

  @Override
  public Collection<String> getPropertyNames() {
    checkForBinding();

    return EdgeInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public Identifiable getLinkProperty(String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();
    EdgeInternal.checkPropertyName(fieldName);

    super.setProperty(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object propertyValue, PropertyType types) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    super.setProperty(name, propertyValue, types);
  }

  @Override
  public <T> List<T> getOrCreateEmbeddedList(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedList(name);
  }

  @Override
  public <T> Set<T> getOrCreateEmbeddedSet(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedSet(name);
  }

  @Override
  public <T> Map<String, T> getOrCreateEmbeddedMap(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedMap(name);
  }

  @Override
  public List<Identifiable> getOrCreateLinkList(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkList(name);
  }

  @Override
  public Set<Identifiable> getOrCreateLinkSet(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkSet(name);
  }

  @Override
  public Map<String, Identifiable> getOrCreateLinkMap(String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkMap(name);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    EdgeInternal.checkPropertyName(fieldName);

    return super.removeProperty(fieldName);
  }

  public static void deleteLinks(Edge delegate) {
    var from = delegate.getFrom();
    if (from != null) {
      VertexInternal.removeOutgoingEdge(from, delegate);
    }
    var to = delegate.getTo();
    if (to != null) {
      VertexInternal.removeIncomingEdge(to, delegate);
    }
  }
}
