package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.Map;
import java.util.Optional;

public class ExecutionResultSet implements ResultSet {

  private final ExecutionStream stream;
  private final CommandContext context;
  private final Optional<ExecutionPlan> plan;

  public ExecutionResultSet(
      ExecutionStream stream, CommandContext context, ExecutionPlan plan) {
    super();
    this.stream = stream;
    this.context = context;
    this.plan = Optional.of(plan);
  }

  @Override
  public boolean hasNext() {
    return stream.hasNext(context);
  }

  @Override
  public Result next() {
    return stream.next(context);
  }

  @Override
  public void close() {
    stream.close(context);
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
