package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public class EmptyStep extends AbstractExecutionStep {

  public EmptyStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return ExecutionStream.empty();
  }

  public boolean canBeCached() {
    return false;
    // DON'T TOUCH!
    // This step is there most of the cases because the query was early optimized based on DATA, eg.
    // an empty cluster,
    // so this execution plan cannot be cached!!!
  }
}
