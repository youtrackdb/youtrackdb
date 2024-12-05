package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 */
public class OFuncitonAggregationContext implements AggregationContext {

  private final OSQLFunction aggregateFunction;
  private List<SQLExpression> params;

  public OFuncitonAggregationContext(OSQLFunction function, List<SQLExpression> params) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
  }

  @Override
  public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override
  public void apply(YTResult next, CommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (SQLExpression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
