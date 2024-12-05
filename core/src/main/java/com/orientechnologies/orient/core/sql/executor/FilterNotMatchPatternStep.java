package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.List;

public class FilterNotMatchPatternStep extends AbstractExecutionStep {

  private final List<AbstractExecutionStep> subSteps;

  public FilterNotMatchPatternStep(
      List<AbstractExecutionStep> steps, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.subSteps = steps;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private YTResult filterMap(YTResult result, OCommandContext ctx) {
    if (!matchesPattern(result, ctx)) {
      return result;
    }
    return null;
  }

  private boolean matchesPattern(YTResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = createExecutionPlan(nextItem, ctx);
    OExecutionStream rs = plan.start();
    try {
      return rs.hasNext(ctx);
    } finally {
      rs.close(ctx);
    }
  }

  private OSelectExecutionPlan createExecutionPlan(YTResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    var db = ctx.getDatabase();
    plan.chain(
        new AbstractExecutionStep(ctx, profilingEnabled) {

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            return OExecutionStream.singleton(copy(nextItem));
          }

          private YTResult copy(YTResult nextItem) {
            YTResultInternal result = new YTResultInternal(db);
            for (String prop : nextItem.getPropertyNames()) {
              result.setProperty(prop, nextItem.getProperty(prop));
            }
            for (String md : nextItem.getMetadataKeys()) {
              result.setMetadata(md, nextItem.getMetadata(md));
            }
            return result;
          }
        });
    subSteps.forEach(plan::chain);
    return plan;
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    //noinspection unchecked,rawtypes
    return (List) subSteps;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ NOT (\n");
    this.subSteps.forEach(x -> result.append(x.prettyPrint(depth + 1, indent)).append("\n"));
    result.append(spaces);
    result.append("  )");
    return result.toString();
  }

  @Override
  public void close() {
    super.close();
  }
}
