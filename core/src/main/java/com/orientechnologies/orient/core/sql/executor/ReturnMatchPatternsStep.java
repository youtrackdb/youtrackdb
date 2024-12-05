package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class ReturnMatchPatternsStep extends AbstractExecutionStep {

  public ReturnMatchPatternsStep(OCommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult next, OCommandContext ctx) {
    next.getPropertyNames().stream()
        .filter(s -> s.startsWith(OMatchExecutionPlanner.DEFAULT_ALIAS_PREFIX))
        .forEach(((YTResultInternal) next)::removeProperty);
    return next;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ RETURN $patterns";
  }
}
