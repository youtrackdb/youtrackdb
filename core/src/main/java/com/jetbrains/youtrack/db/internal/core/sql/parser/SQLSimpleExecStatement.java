package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.SingleOpExecutionPlan;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner. All the
 * execution is delegated to the statement itself, with the execute(ctx) method.
 */
public abstract class SQLSimpleExecStatement extends SQLStatement {

  public SQLSimpleExecStatement(int id) {
    super(id);
  }

  public SQLSimpleExecStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  public abstract ExecutionStream executeSimple(CommandContext ctx);

  public ResultSet execute(
      DatabaseSessionInternal db,
      Object[] args,
      CommandContext parentContext,
      boolean usePlanCache) {
    BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return new ExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public ResultSet execute(
      DatabaseSessionInternal db,
      Map params,
      CommandContext parentContext,
      boolean usePlanCache) {
    BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return new ExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public InternalExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    return new SingleOpExecutionPlan(ctx, this);
  }
}
