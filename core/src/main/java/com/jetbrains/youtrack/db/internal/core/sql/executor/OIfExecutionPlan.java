package com.jetbrains.youtrack.db.internal.core.sql.executor;

/**
 *
 */

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class OIfExecutionPlan implements OInternalExecutionPlan {

  private String location;

  private final CommandContext ctx;

  @Override
  public CommandContext getContext() {
    return ctx;
  }

  protected IfStep step;

  public OIfExecutionPlan(CommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void reset(CommandContext ctx) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    step.close();
  }

  @Override
  public ExecutionStream start() {
    return step.start(ctx);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return step.prettyPrint(depth, indent);
  }

  public void chain(IfStep step) {
    this.step = step;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return Collections.singletonList(step);
  }

  public void setSteps(List<ExecutionStepInternal> steps) {
    this.step = (IfStep) steps.get(0);
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("type", "IfExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", Collections.singletonList(step.toResult(db)));
    return result;
  }

  @Override
  public long getCost() {
    return 0L;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public ExecutionStepInternal executeUntilReturn() {
    OScriptExecutionPlan plan = step.producePlan(ctx);
    if (plan != null) {
      return plan.executeUntilReturn();
    } else {
      return null;
    }
  }

  public boolean containsReturn() {
    return step.containsReturn();
  }
}
