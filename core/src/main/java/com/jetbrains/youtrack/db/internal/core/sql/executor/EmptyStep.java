package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class EmptyStep extends AbstractExecutionStep {

  public EmptyStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return OExecutionStream.empty();
  }

  public boolean canBeCached() {
    return false;
    // DON'T TOUCH!
    // This step is there most of the cases because the query was early optimized based on DATA, eg.
    // an empty cluster,
    // so this execution plan cannot be cached!!!
  }
}
