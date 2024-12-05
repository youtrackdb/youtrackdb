package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;

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
      OCommandContext ctx,
      OCommandContext subCtx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.subExecuitonPlan = subExecutionPlan;
    this.sameContextAsParent = (ctx == subCtx);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    OExecutionStream parentRs = subExecuitonPlan.start();
    return parentRs.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
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
  public OExecutionStep copy(OCommandContext ctx) {
    return new SubQueryStep(subExecuitonPlan.copy(ctx), ctx, ctx, profilingEnabled);
  }
}
