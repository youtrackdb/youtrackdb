package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Optional;
import javax.annotation.Nullable;

public class EntityImplEmbedded extends EntityImpl {

  public EntityImplEmbedded() {
    super();
  }

  public EntityImplEmbedded(String clazz) {
    super(clazz);
    checkEmbeddable();
  }

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
    var doc = new EntityImplEmbedded();
    RecordInternal.unsetDirty(doc);
    var newDoc = (EntityImplEmbedded) copyTo(doc);
    newDoc.dirty = true;
    return newDoc;
  }

  @Override
  public void save() {
    throw new UnsupportedOperationException("Cannot save embedded document");
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
    throw new UnsupportedOperationException("Cannot unload embedded document");
  }

  @Override
  public EntityImpl reset() {
    throw new UnsupportedOperationException("Cannot reset embedded document");
  }
}
