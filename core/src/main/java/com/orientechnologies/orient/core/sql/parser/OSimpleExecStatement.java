package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OSingleOpExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.YTExecutionResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner. All the
 * execution is delegated to the statement itself, with the execute(ctx) method.
 */
public abstract class OSimpleExecStatement extends OStatement {

  public OSimpleExecStatement(int id) {
    super(id);
  }

  public OSimpleExecStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OExecutionStream executeSimple(OCommandContext ctx);

  public YTResultSet execute(
      YTDatabaseSessionInternal db,
      Object[] args,
      OCommandContext parentContext,
      boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
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
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public YTResultSet execute(
      YTDatabaseSessionInternal db,
      Map params,
      OCommandContext parentContext,
      boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new OSingleOpExecutionPlan(ctx, this);
  }
}
