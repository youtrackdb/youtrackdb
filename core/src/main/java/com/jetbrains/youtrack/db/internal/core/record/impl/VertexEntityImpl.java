package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VertexEntityImpl extends EntityImpl implements VertexInternal {

  public VertexEntityImpl(DatabaseSessionInternal db) {
    super(db);
  }

  public VertexEntityImpl(DatabaseSessionInternal database, RID rid) {
    super(database, (RecordId) rid);
  }

  public VertexEntityImpl(DatabaseSessionInternal session, String klass) {
    super(session, klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException(getClassName() + " is not a vertex class");
    }
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

    setProperty(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object propertyValue, PropertyType types) {
    checkForBinding();

    EdgeInternal.checkPropertyName(name);

    super.setProperty(name, propertyValue, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    EdgeInternal.checkPropertyName(fieldName);

    return removePropertyInternal(fieldName);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction) {
    checkForBinding();
    return VertexInternal.super.getVertices(direction);
  }

  @Override
  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  public VertexEntityImpl copy() {
    checkForBinding();

    var newEntity = new VertexEntityImpl(getSession());
    RecordInternal.unsetDirty(newEntity);
    copyTo(newEntity);
    newEntity.dirty = 1;

    return newEntity;
  }

  @Override
  @Nonnull
  public EntityImpl getBaseEntity() {
    return this;
  }
}
