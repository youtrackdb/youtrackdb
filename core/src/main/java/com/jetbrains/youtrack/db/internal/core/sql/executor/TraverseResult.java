package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public class TraverseResult extends ResultInternal {

  protected Integer depth;

  public TraverseResult(DatabaseSessionInternal session) {
    super(session);
  }

  public TraverseResult(DatabaseSessionInternal db, Identifiable element) {
    super(db, element);
  }

  @Override
  public <T> T getProperty(String name) {
    assert session == null || session.assertIfNotActive();
    if ("$depth".equalsIgnoreCase(name)) {
      return (T) depth;
    }
    return super.getProperty(name);
  }

  @Override
  public void setProperty(String name, Object value) {
    assert session == null || session.assertIfNotActive();
    if ("$depth".equalsIgnoreCase(name)) {
      if (value instanceof Number) {
        depth = ((Number) value).intValue();
      }
    } else {
      super.setProperty(name, value);
    }
  }
}
