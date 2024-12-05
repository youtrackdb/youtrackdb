package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;

/**
 *
 */
public class ODeleteExecutionPlan extends OUpdateExecutionPlan {

  public ODeleteExecutionPlan(OCommandContext ctx) {
    super(ctx);
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal res = (YTResultInternal) super.toResult(db);
    res.setProperty("type", "DeleteExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    for (OExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public OInternalExecutionPlan copy(OCommandContext ctx) {
    ODeleteExecutionPlan copy = new ODeleteExecutionPlan(ctx);
    super.copyOn(copy, ctx);
    return copy;
  }
}
