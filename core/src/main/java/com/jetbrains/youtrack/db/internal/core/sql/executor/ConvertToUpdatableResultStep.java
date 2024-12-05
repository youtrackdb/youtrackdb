package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 * takes a normal result set and transforms it in another result set made of OUpdatableRecord
 * instances. Records that are not identifiable are discarded.
 *
 * <p>This is the opposite of ConvertToResultInternalStep
 */
public class ConvertToUpdatableResultStep extends AbstractExecutionStep {

  public ConvertToUpdatableResultStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    ExecutionStream resultSet = prev.start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private YTResult filterMap(YTResult result, CommandContext ctx) {
    if (result instanceof YTUpdatableResult) {
      return result;
    }
    if (result.isEntity()) {
      var element = result.toEntity();
      if (element != null) {
        return new YTUpdatableResult(ctx.getDatabase(), element);
      }
      return result;
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CONVERT TO UPDATABLE ITEM";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
