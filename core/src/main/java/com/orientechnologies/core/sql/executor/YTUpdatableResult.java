package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.record.impl.YTEntityInternal;

/**
 *
 */
public class YTUpdatableResult extends YTResultInternal {

  protected YTResultInternal previousValue = null;

  public YTUpdatableResult(YTDatabaseSessionInternal session, YTEntity element) {
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
      result = ((YTEntity) identifiable).getProperty(name);
    }
    if (result instanceof YTIdentifiable && ((YTIdentifiable) result).getIdentity()
        .isPersistent()) {
      result = (T) ((YTIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public YTEntity toEntity() {
    return (YTEntity) identifiable;
  }

  @Override
  public void setProperty(String name, Object value) {
    ((YTEntityInternal) identifiable).setPropertyInternal(name, value);
  }

  public void removeProperty(String name) {
    ((YTEntity) identifiable).removeProperty(name);
  }
}
