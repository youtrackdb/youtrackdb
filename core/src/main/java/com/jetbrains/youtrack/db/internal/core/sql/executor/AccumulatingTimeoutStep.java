package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.TimeoutResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OTimeout;

/**
 *
 */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {

  private final OTimeout timeout;

  public AccumulatingTimeoutStep(OTimeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    final ExecutionStream internal = prev.start(ctx);
    return new TimeoutResultSet(internal, this.timeout.getVal().longValue(), this::fail);
  }

  private void fail() {
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      // do nothing
    } else {
      sendTimeout();
      throw new YTTimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy(), ctx, profilingEnabled);
  }

  @Override
  public void reset() {
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + "ms)";
  }
}
