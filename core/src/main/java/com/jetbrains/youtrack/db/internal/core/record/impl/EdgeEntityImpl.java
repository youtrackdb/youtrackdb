package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.StatefulEdge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EdgeEntityImpl extends EntityImpl implements EdgeInternal, StatefulEdge {

  public EdgeEntityImpl(DatabaseSessionInternal database, RecordId rid) {
    super(database, rid);
  }

  @Nullable
  @Override
  public Vertex getFrom() {
    var result = getPropertyInternal(DIRECTION_OUT);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.castToVertex();
  }

  @Override
  public boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }

    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    var typeClass = getImmutableSchemaClass(session);
    var session = getSession();

    types.add(typeClass.getName(session));
    typeClass.getAllSuperClasses().stream().map(schemaClass -> schemaClass.getName(session))
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


  @Nullable
  @Override
  public Identifiable getFromLink() {
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_OUT);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType(db)) {
      return rid;
    }

    return null;
  }

  @Nullable
  @Override
  public Vertex getTo() {
    var result = getPropertyInternal(DIRECTION_IN);
    if (!(result instanceof Entity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.castToVertex();
  }

  @Nullable
  @Override
  public Identifiable getToLink() {
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_IN);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType(db)) {
      return rid;
    }

    return null;
  }

  @Override
  public boolean isLightweight() {
    // LIGHTWEIGHT EDGES MANAGED BY EdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }


  @Override
  public @Nonnull Collection<String> getPropertyNames() {
    checkForBinding();

    return EdgeInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(@Nonnull String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public RID getLink(@Nonnull String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return super.getLink(fieldName);
  }

  @Override
  public void setProperty(@Nonnull String propertyName, @Nullable Object propertyValue) {
    checkForBinding();
    EdgeInternal.checkPropertyName(propertyName);

    super.setProperty(propertyName, propertyValue);
  }

  @Override
  public void setProperty(@Nonnull String propertyName, Object propertyValue,
      @Nonnull PropertyType type) {
    checkForBinding();
    EdgeInternal.checkPropertyName(propertyName);

    super.setProperty(propertyName, propertyValue, type);
  }

  @Override
  public @Nonnull <T> List<T> getOrCreateEmbeddedList(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedList(name);
  }

  @Override
  public @Nonnull <T> Set<T> getOrCreateEmbeddedSet(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedSet(name);
  }

  @Override
  public @Nonnull <T> Map<String, T> getOrCreateEmbeddedMap(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateEmbeddedMap(name);
  }

  @Override
  public @Nonnull List<Identifiable> getOrCreateLinkList(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkList(name);
  }

  @Nonnull
  @Override
  public Set<Identifiable> getOrCreateLinkSet(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkSet(name);
  }

  @Nonnull
  @Override
  public Map<String, Identifiable> getOrCreateLinkMap(@Nonnull String name) {
    checkForBinding();
    EdgeInternal.checkPropertyName(name);

    return super.getOrCreateLinkMap(name);
  }

  @Override
  public <RET> RET removeProperty(@Nonnull String fieldName) {
    checkForBinding();
    EdgeInternal.checkPropertyName(fieldName);

    return super.removeProperty(fieldName);
  }

  @Nonnull
  @Override
  public SchemaClass getSchemaClass() {
    return super.getSchemaClass();
  }

  @Nonnull
  @Override
  public String getSchemaClassName() {
    return super.getSchemaClassName();
  }

  public static void deleteLinks(DatabaseSessionInternal db, Edge delegate) {
    var from = delegate.getFrom();
    if (from != null) {
      VertexInternal.removeOutgoingEdge(db, from, delegate);
    }

    var to = delegate.getTo();
    if (to != null) {
      VertexInternal.removeIncomingEdge(db, to, delegate);
    }
  }
}
