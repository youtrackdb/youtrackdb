package com.orientechnologies.core.sql.executor;

/**
 *
 */

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.parser.WhileStep;

/**
 *
 */
public class ORetryExecutionPlan extends OUpdateExecutionPlan {

  public ORetryExecutionPlan(OCommandContext ctx) {
    super(ctx);
  }

  public boolean containsReturn() {
    for (OExecutionStep step : getSteps()) {
      if (step instanceof ForEachStep) {
        return ((ForEachStep) step).containsReturn();
      }
      if (step instanceof WhileStep) {
        return ((WhileStep) step).containsReturn();
      }
    }

    return false;
  }
}
