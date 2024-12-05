package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.Map;
import java.util.Optional;

public class YTExecutionResultSet implements YTResultSet {

  private final OExecutionStream stream;
  private final OCommandContext context;
  private final Optional<OExecutionPlan> plan;

  public YTExecutionResultSet(
      OExecutionStream stream, OCommandContext context, OExecutionPlan plan) {
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
