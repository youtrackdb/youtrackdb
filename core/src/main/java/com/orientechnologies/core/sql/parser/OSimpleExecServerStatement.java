package com.orientechnologies.core.sql.parser;

import com.orientechnologies.core.command.OBasicServerCommandContext;
import com.orientechnologies.core.command.OServerCommandContext;
import com.orientechnologies.core.db.YouTrackDBInternal;
import com.orientechnologies.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.core.sql.executor.OSingleOpServerExecutionPlan;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.YTExecutionResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner. All the
 * execution is delegated to the statement itself, with the execute(ctx) method.
 */
public abstract class OSimpleExecServerStatement extends OServerStatement {

  public OSimpleExecServerStatement(int id) {
    super(id);
  }

  public OSimpleExecServerStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OExecutionStream executeSimple(OServerCommandContext ctx);

  public YTResultSet execute(
      YouTrackDBInternal db,
      Object[] args,
      OServerCommandContext parentContext,
      boolean usePlanCache) {
    OBasicServerCommandContext ctx = new OBasicServerCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setServer(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    OSingleOpServerExecutionPlan executionPlan =
        (OSingleOpServerExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(), ctx, executionPlan);
  }

  public YTResultSet execute(
      YouTrackDBInternal db, Map params, OServerCommandContext parentContext,
      boolean usePlanCache) {
    OBasicServerCommandContext ctx = new OBasicServerCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setServer(db);
    ctx.setInputParameters(params);
    OSingleOpServerExecutionPlan executionPlan =
        (OSingleOpServerExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(), ctx, executionPlan);
  }

  public OInternalExecutionPlan createExecutionPlan(
      OServerCommandContext ctx, boolean enableProfiling) {
    return new OSingleOpServerExecutionPlan(ctx, this);
  }
}
