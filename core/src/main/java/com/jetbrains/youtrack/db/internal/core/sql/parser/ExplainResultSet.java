package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.query.ExecutionPlan;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseStats;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 *
 */
public class ExplainResultSet implements ResultSet {

  private final ExecutionPlan executionPlan;
  private final DatabaseStats dbStats;
  boolean hasNext = true;
  @Nullable
  private final DatabaseSessionInternal session;

  public ExplainResultSet(@Nullable DatabaseSessionInternal session, ExecutionPlan executionPlan,
      DatabaseStats dbStats) {
    this.executionPlan = executionPlan;
    this.dbStats = dbStats;
    this.session = session;

    assert session == null || session.assertIfNotActive();
  }

  @Override
  public boolean hasNext() {
    assert session == null || session.assertIfNotActive();
    return hasNext;
  }

  @Override
  public Result next() {
    assert session == null || session.assertIfNotActive();
    if (!hasNext) {
      throw new IllegalStateException();
    }

    var result = new ResultInternal(session);
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.toResult(
        session)));
    getExecutionPlan()
        .ifPresent(x -> result.setProperty("executionPlanAsString", x.prettyPrint(0, 3)));
    getExecutionPlan().ifPresent(x -> result.setProperty("dbStats", dbStats.toResult(
        session)));

    hasNext = false;
    return result;
  }

  @Override
  public void close() {
    assert session == null || session.assertIfNotActive();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    assert session == null || session.assertIfNotActive();
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    assert session == null || session.assertIfNotActive();
    return new HashMap<>();
  }
}
