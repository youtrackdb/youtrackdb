package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class IteratorResultSet implements ResultSet {

  protected final Iterator iterator;
  protected final DatabaseSessionInternal db;

  public IteratorResultSet(DatabaseSessionInternal db, Iterator iter) {
    this.iterator = iter;
    this.db = db;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Result next() {
    Object val = iterator.next();
    if (val instanceof Result) {
      return (Result) val;
    }

    ResultInternal result;
    if (val instanceof Identifiable) {
      result = new ResultInternal(db, (Identifiable) val);
    } else {
      result = new ResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close() {
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
