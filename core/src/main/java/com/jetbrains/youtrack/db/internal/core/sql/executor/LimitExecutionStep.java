package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OLimit;

/**
 *
 */
public class LimitExecutionStep extends AbstractExecutionStep {

  private final OLimit limit;

  public LimitExecutionStep(OLimit limit, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.limit = limit;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    int limitVal = limit.getValue(ctx);
    if (limitVal == -1) {
      return prev.start(ctx);
    }
    OExecutionStream result = prev.start(ctx);
    return result.limit(limitVal);
  }

  @Override
  public void sendTimeout() {
  }

  @Override
  public void close() {
    if (prev != null) {
      prev.close();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ LIMIT (" + limit.toString() + ")";
  }
}
