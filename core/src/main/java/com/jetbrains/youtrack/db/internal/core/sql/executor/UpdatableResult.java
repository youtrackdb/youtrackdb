package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;

/**
 *
 */
public class UpdatableResult extends ResultInternal {

  protected ResultInternal previousValue = null;

  public UpdatableResult(DatabaseSessionInternal session, Entity element) {
    super(session, element);
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  public <T> T getProperty(String name) {
    loadIdentifiable();
    T result = null;
    if (content != null && content.containsKey(name)) {
      result = (T) content.get(name);
    } else if (this.isEntity()) {
      result = ((Entity) identifiable).getProperty(name);
    }
    if (result instanceof Identifiable && ((Identifiable) result).getIdentity()
        .isPersistent()) {
      result = (T) ((Identifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public Entity toEntity() {
    return (Entity) identifiable;
  }

  @Override
  public void setProperty(String name, Object value) {
    ((EntityInternal) identifiable).setPropertyInternal(name, value);
  }

  public void removeProperty(String name) {
    ((Entity) identifiable).removeProperty(name);
  }
}
