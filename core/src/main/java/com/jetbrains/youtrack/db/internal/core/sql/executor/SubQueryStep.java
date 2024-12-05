package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public class SubQueryStep extends AbstractExecutionStep {

  private final OInternalExecutionPlan subExecuitonPlan;
  private final boolean sameContextAsParent;

  /**
   * executes a sub-query
   *
   * @param subExecutionPlan the execution plan of the sub-query
   * @param ctx              the context of the current execution plan
   * @param subCtx           the context of the subquery execution plan
   */
  public SubQueryStep(
      OInternalExecutionPlan subExecutionPlan,
      CommandContext ctx,
      CommandContext subCtx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.subExecuitonPlan = subExecutionPlan;
    this.sameContextAsParent = (ctx == subCtx);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    ExecutionStream parentRs = subExecuitonPlan.start();
    return parentRs.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, CommandContext ctx) {
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM SUBQUERY \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  @Override
  public boolean canBeCached() {
    return sameContextAsParent && subExecuitonPlan.canBeCached();
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new SubQueryStep(subExecuitonPlan.copy(ctx), ctx, ctx, profilingEnabled);
  }
}
