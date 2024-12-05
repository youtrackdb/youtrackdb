package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;

/**
 *
 */
public class ODeleteExecutionPlan extends OUpdateExecutionPlan {

  public ODeleteExecutionPlan(CommandContext ctx) {
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
    for (ExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public OInternalExecutionPlan copy(CommandContext ctx) {
    ODeleteExecutionPlan copy = new ODeleteExecutionPlan(ctx);
    super.copyOn(copy, ctx);
    return copy;
  }
}
