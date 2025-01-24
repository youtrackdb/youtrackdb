package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import java.lang.ref.WeakReference;
import java.util.Optional;
import javax.annotation.Nullable;

public class EmbeddedEntityImpl extends EntityImpl {

  public EmbeddedEntityImpl(String clazz, DatabaseSessionInternal session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public EmbeddedEntityImpl(DatabaseSessionInternal session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public EmbeddedEntityImpl copy() {
    var entity = new EmbeddedEntityImpl(getSession());
    RecordInternal.unsetDirty(entity);
    var newEntity = (EmbeddedEntityImpl) copyTo(entity);
    newEntity.dirty = 1;
    return newEntity;
  }

  @Override
  protected void addOwner(RecordElement iOwner) {
    checkForBinding();

    if (iOwner == null) {
      return;
    }

    this.owner = new WeakReference<>(iOwner);
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
