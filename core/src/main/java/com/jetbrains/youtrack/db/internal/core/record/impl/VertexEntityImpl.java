package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
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
    if (!getImmutableSchemaClass(session).isVertexType(session)) {
      throw new IllegalArgumentException(getSchemaClassName() + " is not a vertex class");
    }
  }

  @Override
  public @Nonnull Collection<String> getPropertyNames() {
    checkForBinding();

    return VertexInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(@Nonnull String fieldName) {
    checkForBinding();

    VertexInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public RID getLink(@Nonnull String fieldName) {
    checkForBinding();

    VertexInternal.checkPropertyName(fieldName);

    return super.getLink(fieldName);
  }

  @Override
  public void setProperty(@Nonnull String fieldName, @Nullable Object propertyValue) {
    checkForBinding();

    VertexInternal.checkPropertyName(fieldName);
    super.setProperty(fieldName, propertyValue);
  }

  @Override
  public void setProperty(@Nonnull String name, Object propertyValue, @Nonnull PropertyType type) {
    checkForBinding();

    VertexInternal.checkPropertyName(name);
    super.setProperty(name, propertyValue, type);
  }

  @Override
  public <RET> RET removeProperty(@Nonnull String fieldName) {
    checkForBinding();

    VertexInternal.checkPropertyName(fieldName);
    return removePropertyInternal(fieldName);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction) {
    checkForBinding();
    return VertexInternal.super.getVertices(direction);
  }


  @Override
  @Nonnull
  public EntityImpl getBaseEntity() {
    return this;
  }
}
