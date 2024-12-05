package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OGroupBy;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OProjectionItem;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final OGroupBy groupBy;
  private final long timeoutMillis;
  private final long limit;

  public AggregateProjectionCalculationStep(
      OProjection projection,
      OGroupBy groupBy,
      long limit,
      OCommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    super(projection, ctx, profilingEnabled);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    List<YTResult> finalResults = executeAggregation(ctx);
    return OExecutionStream.resultIterator(finalResults.iterator());
  }

  private List<YTResult> executeAggregation(OCommandContext ctx) {
    long timeoutBegin = System.currentTimeMillis();
    if (prev == null) {
      throw new YTCommandExecutionException(
          "Cannot execute an aggregation or a GROUP BY without a previous result");
    }

    OExecutionStepInternal prevStep = prev;
    OExecutionStream lastRs = prevStep.start(ctx);
    Map<List<?>, YTResultInternal> aggregateResults = new LinkedHashMap<>();
    while (lastRs.hasNext(ctx)) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      aggregate(lastRs.next(ctx), ctx, aggregateResults);
    }
    lastRs.close(ctx);
    List<YTResult> finalResults = new ArrayList<>(aggregateResults.values());
    aggregateResults.clear();
    for (YTResult ele : finalResults) {
      YTResultInternal item = (YTResultInternal) ele;
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      for (String name : item.getTemporaryProperties()) {
        Object prevVal = item.getTemporaryProperty(name);
        if (prevVal instanceof AggregationContext) {
          item.setTemporaryProperty(name, ((AggregationContext) prevVal).getFinalValue());
        }
      }
    }
    return finalResults;
  }

  private void aggregate(
      YTResult next, OCommandContext ctx, Map<List<?>, YTResultInternal> aggregateResults) {
    var db = ctx.getDatabase();
    List<Object> key = new ArrayList<>();
    if (groupBy != null) {
      for (OExpression item : groupBy.getItems()) {
        Object val = item.execute(next, ctx);
        key.add(val);
      }
    }
    YTResultInternal preAggr = aggregateResults.get(key);
    if (preAggr == null) {
      if (limit > 0 && aggregateResults.size() > limit) {
        return;
      }
      preAggr = new YTResultInternal(ctx.getDatabase());

      for (OProjectionItem proj : this.projection.getItems()) {
        String alias = proj.getProjectionAlias().getStringValue();
        if (!proj.isAggregate(db)) {
          preAggr.setProperty(alias, proj.execute(next, ctx));
        }
      }
      aggregateResults.put(key, preAggr);
    }

    for (OProjectionItem proj : this.projection.getItems()) {
      String alias = proj.getProjectionAlias().getStringValue();
      if (proj.isAggregate(db)) {
        AggregationContext aggrCtx = (AggregationContext) preAggr.getTemporaryProperty(alias);
        if (aggrCtx == null) {
          aggrCtx = proj.getAggregationContext(ctx);
          preAggr.setTemporaryProperty(alias, aggrCtx);
        }
        aggrCtx.apply(next, ctx);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result +=
        "\n"
            + spaces
            + "      "
            + projection.toString()
            + (groupBy == null ? "" : (spaces + "\n  " + groupBy));
    return result;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new AggregateProjectionCalculationStep(
        projection.copy(),
        groupBy == null ? null : groupBy.copy(),
        limit,
        ctx,
        timeoutMillis,
        profilingEnabled);
  }
}
