package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OSelectExecutionPlan subExecuitonPlan;
  private final String nodeName;

  public DistributedExecutionStep(
      OSelectExecutionPlan subExecutionPlan,
      String nodeName,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    ExecutionStream remote = sendSerializedExecutionPlan(nodeName, subExecuitonPlan, ctx);
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return remote;
  }

  private ExecutionStream sendSerializedExecutionPlan(
      String nodeName, OExecutionPlan serializedExecutionPlan, CommandContext ctx) {
    YTDatabaseSessionInternal db = ctx.getDatabase();
    return ExecutionStream.resultIterator(
        db.queryOnNode(nodeName, serializedExecutionPlan, ctx.getInputParameters()).stream()
            .iterator());
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXECUTE ON NODE ").append(nodeName).append("----------- \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    builder.append("  ------------------------------------------- \n");
    builder.append("   |\n");
    builder.append("   V\n");
    return builder.toString();
  }
}
