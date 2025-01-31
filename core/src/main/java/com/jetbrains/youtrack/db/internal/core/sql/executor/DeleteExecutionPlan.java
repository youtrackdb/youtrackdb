package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public class DeleteExecutionPlan extends UpdateExecutionPlan {

  public DeleteExecutionPlan(CommandContext ctx) {
    super(ctx);
  }

  @Override
  public Result toResult(DatabaseSession db) {
    var res = (ResultInternal) super.toResult(db);
    res.setProperty("type", "DeleteExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    for (var step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public InternalExecutionPlan copy(CommandContext ctx) {
    var copy = new DeleteExecutionPlan(ctx);
    super.copyOn(copy, ctx);
    return copy;
  }
}
