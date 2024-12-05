package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Collection;

/**
 * unwinds a result-set.
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  public AbstractUnrollStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException("Cannot expand without a target");
    }
    ExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(this::fetchNextResults);
  }

  private ExecutionStream fetchNextResults(YTResult res, CommandContext ctx) {
    return ExecutionStream.resultIterator(unroll(res, ctx).iterator());
  }

  protected abstract Collection<YTResult> unroll(final YTResult doc,
      final CommandContext iContext);
}
