package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.List;

/**
 *
 */
public class MatchFirstStep extends AbstractExecutionStep {

  private final PatternNode node;
  private final OInternalExecutionPlan executionPlan;

  public MatchFirstStep(CommandContext context, PatternNode node, boolean profilingEnabled) {
    this(context, node, null, profilingEnabled);
  }

  public MatchFirstStep(
      CommandContext context,
      PatternNode node,
      OInternalExecutionPlan subPlan,
      boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.node = node;
    this.executionPlan = subPlan;
  }

  @Override
  public void reset() {
    if (executionPlan != null) {
      executionPlan.reset(ctx);
    }
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    ExecutionStream data;
    String alias = getAlias();

    @SuppressWarnings("unchecked")
    List<YTResult> matchedNodes =
        (List<YTResult>) ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    if (matchedNodes != null) {
      data = ExecutionStream.resultIterator(matchedNodes.iterator());
    } else {
      data = executionPlan.start();
    }

    return data.map(
        (result, context) -> {
          YTResultInternal newResult = new YTResultInternal(context.getDatabase());
          newResult.setProperty(getAlias(), result);
          context.setVariable("$matched", newResult);
          return newResult;
        });
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET \n");
    result.append(spaces);
    result.append("   ");
    result.append(getAlias());
    if (executionPlan != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  AS\n");
      result.append(executionPlan.prettyPrint(depth + 1, indent));
    }

    return result.toString();
  }

  private String getAlias() {
    return this.node.alias;
  }
}
