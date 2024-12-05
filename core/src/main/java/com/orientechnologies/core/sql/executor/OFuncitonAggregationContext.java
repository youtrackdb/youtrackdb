package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.functions.OSQLFunction;
import com.orientechnologies.core.sql.parser.OExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 */
public class OFuncitonAggregationContext implements AggregationContext {

  private final OSQLFunction aggregateFunction;
  private List<OExpression> params;

  public OFuncitonAggregationContext(OSQLFunction function, List<OExpression> params) {
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
  public void apply(YTResult next, OCommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (OExpression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
