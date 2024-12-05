package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.OExpireResultSet;
import com.orientechnologies.core.sql.parser.OTimeout;

/**
 *
 */
public class TimeoutStep extends AbstractExecutionStep {

  private final OTimeout timeout;

  public TimeoutStep(OTimeout timeout, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    return new OExpireResultSet(prev.start(ctx), timeout.getVal().longValue(), this::fail);
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
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + " millis)";
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new TimeoutStep(this.timeout.copy(), ctx, profilingEnabled);
  }
}
