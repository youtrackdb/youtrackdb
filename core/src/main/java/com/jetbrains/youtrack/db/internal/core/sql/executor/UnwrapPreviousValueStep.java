package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  public UnwrapPreviousValueStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result instanceof YTUpdatableResult) {
      result = ((YTUpdatableResult) result).previousValue;
      if (result == null) {
        throw new YTCommandExecutionException(
            "Invalid status of record: no previous value available");
      }
      return result;
    } else {
      throw new YTCommandExecutionException(
          "Invalid status of record: no previous value available");
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ UNWRAP PREVIOUS VALUE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
