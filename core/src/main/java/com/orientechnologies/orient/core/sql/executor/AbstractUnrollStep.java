package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Collection;

/**
 * unwinds a result-set.
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  public AbstractUnrollStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev == null) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(this::fetchNextResults);
  }

  private OExecutionStream fetchNextResults(OResult res, OCommandContext ctx) {
    return OExecutionStream.resultIterator(unroll(res, ctx).iterator());
  }

  protected abstract Collection<OResult> unroll(final OResult doc, final OCommandContext iContext);
}
