package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import java.util.Optional;
import javax.annotation.Nullable;

public class EntityImplEmbedded extends EntityImpl {

  public EntityImplEmbedded(String clazz, DatabaseSessionInternal session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public EntityImplEmbedded(DatabaseSessionInternal session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public EntityImplEmbedded copy() {
    var entity = new EntityImplEmbedded(getSession());
    RecordInternal.unsetDirty(entity);
    var newEntity = (EntityImplEmbedded) copyTo(entity);
    newEntity.dirty = true;
    return newEntity;
  }

  @Override
  public void save() {
    throw new UnsupportedOperationException("Cannot save embedded entity");
  }

  @Override
  public Optional<Vertex> asVertex() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public Vertex toVertex() {
    return null;
  }

  @Override
  public Optional<Edge> asEdge() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public Edge toEdge() {
    return null;
  }

  @Override
  public boolean isVertex() {
    return false;
  }

  @Override
  public boolean isEdge() {
    return false;
  }

  @Override
  public void unload() {
    throw new UnsupportedOperationException("Cannot unload embedded entity");
  }

  @Override
  public EntityImpl reset() {
    throw new UnsupportedOperationException("Cannot reset embedded entity");
  }
}
