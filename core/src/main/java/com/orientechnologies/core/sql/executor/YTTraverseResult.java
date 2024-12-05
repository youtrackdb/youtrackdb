package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;

/**
 *
 */
public class YTTraverseResult extends YTResultInternal {

  protected Integer depth;

  public YTTraverseResult(YTDatabaseSessionInternal db) {
    super(db);
  }

  public YTTraverseResult(YTDatabaseSessionInternal db, YTIdentifiable element) {
    super(db, element);
  }

  @Override
  public <T> T getProperty(String name) {
    if ("$depth".equalsIgnoreCase(name)) {
      return (T) depth;
    }
    return super.getProperty(name);
  }

  @Override
  public void setProperty(String name, Object value) {
    if ("$depth".equalsIgnoreCase(name)) {
      if (value instanceof Number) {
        depth = ((Number) value).intValue();
      }
    } else {
      super.setProperty(name, value);
    }
  }
}
