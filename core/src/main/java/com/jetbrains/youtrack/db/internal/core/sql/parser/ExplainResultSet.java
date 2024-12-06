package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseStats;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class ExplainResultSet implements ResultSet {

  private final ExecutionPlan executionPlan;
  private final DatabaseStats dbStats;
  boolean hasNext = true;
  private final DatabaseSessionInternal db;

  public ExplainResultSet(DatabaseSessionInternal db, ExecutionPlan executionPlan,
      DatabaseStats dbStats) {
    this.executionPlan = executionPlan;
    this.dbStats = dbStats;
    this.db = db;

    assert db == null || db.assertIfNotActive();
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public Result next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }

    ResultInternal result = new ResultInternal(db);
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.toResult(db)));
    getExecutionPlan()
        .ifPresent(x -> result.setProperty("executionPlanAsString", x.prettyPrint(0, 3)));
    getExecutionPlan().ifPresent(x -> result.setProperty("dbStats", dbStats.toResult(db)));

    hasNext = false;
    return result;
  }

  @Override
  public void close() {
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
