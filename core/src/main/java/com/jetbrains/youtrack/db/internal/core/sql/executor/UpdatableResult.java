package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import javax.annotation.Nonnull;

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
    assert session == null || session.assertIfNotActive();
    return true;
  }

  public <T> T getProperty(@Nonnull String name) {
    assert session == null || session.assertIfNotActive();
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
  public void setProperty(String name, Object value) {
    assert session == null || session.assertIfNotActive();
    ((EntityInternal) identifiable).setPropertyInternal(name, value);
  }

  public void removeProperty(String name) {
    assert session == null || session.assertIfNotActive();
    ((Entity) identifiable).removeProperty(name);
  }
}
