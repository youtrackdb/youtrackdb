package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class YTIteratorResultSet implements YTResultSet {

  protected final Iterator iterator;
  protected final YTDatabaseSessionInternal db;

  public YTIteratorResultSet(YTDatabaseSessionInternal db, Iterator iter) {
    this.iterator = iter;
    this.db = db;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public YTResult next() {
    Object val = iterator.next();
    if (val instanceof YTResult) {
      return (YTResult) val;
    }

    YTResultInternal result;
    if (val instanceof YTIdentifiable) {
      result = new YTResultInternal(db, (YTIdentifiable) val);
    } else {
      result = new YTResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close() {
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
