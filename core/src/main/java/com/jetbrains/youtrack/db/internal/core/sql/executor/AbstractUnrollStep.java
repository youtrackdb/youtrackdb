package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import java.util.Collection;

/**
 * unwinds a result-set.
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  public AbstractUnrollStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(this::fetchNextResults);
  }

  private OExecutionStream fetchNextResults(YTResult res, OCommandContext ctx) {
    return OExecutionStream.resultIterator(unroll(res, ctx).iterator());
  }

  protected abstract Collection<YTResult> unroll(final YTResult doc,
      final OCommandContext iContext);
}
