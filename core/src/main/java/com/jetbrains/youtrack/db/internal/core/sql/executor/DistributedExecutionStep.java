package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OSelectExecutionPlan subExecuitonPlan;
  private final String nodeName;

  public DistributedExecutionStep(
      OSelectExecutionPlan subExecutionPlan,
      String nodeName,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    OExecutionStream remote = sendSerializedExecutionPlan(nodeName, subExecuitonPlan, ctx);
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return remote;
  }

  private OExecutionStream sendSerializedExecutionPlan(
      String nodeName, OExecutionPlan serializedExecutionPlan, OCommandContext ctx) {
    YTDatabaseSessionInternal db = ctx.getDatabase();
    return OExecutionStream.resultIterator(
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
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXECUTE ON NODE ").append(nodeName).append("----------- \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    builder.append("  ------------------------------------------- \n");
    builder.append("   |\n");
    builder.append("   V\n");
    return builder.toString();
  }
}
