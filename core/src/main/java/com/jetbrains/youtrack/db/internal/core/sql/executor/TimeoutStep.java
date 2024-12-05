package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExpireResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OTimeout;

/**
 *
 */
public class TimeoutStep extends AbstractExecutionStep {

  private final OTimeout timeout;

  public TimeoutStep(OTimeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    return new ExpireResultSet(prev.start(ctx), timeout.getVal().longValue(), this::fail);
  }

  private void fail() {
    sendTimeout();
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
    } else {
      throw new YTTimeoutException("Timeout expired");
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + " millis)";
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new TimeoutStep(this.timeout.copy(), ctx, profilingEnabled);
  }
}
