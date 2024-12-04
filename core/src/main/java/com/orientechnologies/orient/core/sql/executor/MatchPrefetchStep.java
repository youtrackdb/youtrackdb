package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MatchPrefetchStep extends AbstractExecutionStep {

  public static final String PREFETCHED_MATCH_ALIAS_PREFIX = "$$OrientDB_Prefetched_Alias_Prefix__";

  private final String alias;
  private final OInternalExecutionPlan prefetchExecutionPlan;

  public MatchPrefetchStep(
      OCommandContext ctx,
      OInternalExecutionPlan prefetchExecPlan,
      String alias,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.prefetchExecutionPlan = prefetchExecPlan;
    this.alias = alias;
  }

  @Override
  public void reset() {
    prefetchExecutionPlan.reset(ctx);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    OExecutionStream nextBlock = prefetchExecutionPlan.start();
    List<OResult> prefetched = new ArrayList<>();
    while (nextBlock.hasNext(ctx)) {
      prefetched.add(nextBlock.next(ctx));
    }
    nextBlock.close(ctx);
    prefetchExecutionPlan.close();
    ctx.setVariable(PREFETCHED_MATCH_ALIAS_PREFIX + alias, prefetched);
    return OExecutionStream.empty();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces
        + "+ PREFETCH "
        + alias
        + "\n"
        + prefetchExecutionPlan.prettyPrint(depth + 1, indent);
  }
}
