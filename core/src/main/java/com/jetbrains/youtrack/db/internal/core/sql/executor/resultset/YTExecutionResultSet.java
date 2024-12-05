package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.Map;
import java.util.Optional;

public class YTExecutionResultSet implements YTResultSet {

  private final ExecutionStream stream;
  private final CommandContext context;
  private final Optional<OExecutionPlan> plan;

  public YTExecutionResultSet(
      ExecutionStream stream, CommandContext context, OExecutionPlan plan) {
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
  public YTResult next() {
    return stream.next(context);
  }

  @Override
  public void close() {
    stream.close(context);
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
