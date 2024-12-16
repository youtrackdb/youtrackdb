package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.ExecutionStep;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjectionItem;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final SQLGroupBy groupBy;
  private final long timeoutMillis;
  private final long limit;

  public AggregateProjectionCalculationStep(
      SQLProjection projection,
      SQLGroupBy groupBy,
      long limit,
      CommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    super(projection, ctx, profilingEnabled);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    List<Result> finalResults = executeAggregation(ctx);
    return ExecutionStream.resultIterator(finalResults.iterator());
  }

  private List<Result> executeAggregation(CommandContext ctx) {
    long timeoutBegin = System.currentTimeMillis();
    if (prev == null) {
      throw new CommandExecutionException(
          "Cannot execute an aggregation or a GROUP BY without a previous result");
    }

    ExecutionStepInternal prevStep = prev;
    ExecutionStream lastRs = prevStep.start(ctx);
    Map<List<?>, ResultInternal> aggregateResults = new LinkedHashMap<>();
    while (lastRs.hasNext(ctx)) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      aggregate(lastRs.next(ctx), ctx, aggregateResults);
    }
    lastRs.close(ctx);
    List<Result> finalResults = new ArrayList<>(aggregateResults.values());
    aggregateResults.clear();
    for (Result ele : finalResults) {
      ResultInternal item = (ResultInternal) ele;
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
      Result next, CommandContext ctx, Map<List<?>, ResultInternal> aggregateResults) {
    var db = ctx.getDatabase();
    List<Object> key = new ArrayList<>();
    if (groupBy != null) {
      for (SQLExpression item : groupBy.getItems()) {
        Object val = item.execute(next, ctx);
        key.add(val);
      }
    }
    ResultInternal preAggr = aggregateResults.get(key);
    if (preAggr == null) {
      if (limit > 0 && aggregateResults.size() > limit) {
        return;
      }
      preAggr = new ResultInternal(ctx.getDatabase());

      for (SQLProjectionItem proj : this.projection.getItems()) {
        String alias = proj.getProjectionAlias().getStringValue();
        if (!proj.isAggregate(db)) {
          preAggr.setProperty(alias, proj.execute(next, ctx));
        }
      }
      aggregateResults.put(key, preAggr);
    }

    for (SQLProjectionItem proj : this.projection.getItems()) {
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
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
  public ExecutionStep copy(CommandContext ctx) {
    return new AggregateProjectionCalculationStep(
        projection.copy(),
        groupBy == null ? null : groupBy.copy(),
        limit,
        ctx,
        timeoutMillis,
        profilingEnabled);
  }
}
