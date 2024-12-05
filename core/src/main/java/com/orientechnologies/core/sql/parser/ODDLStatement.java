package com.orientechnologies.core.sql.parser;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.ODDLExecutionPlan;
import com.orientechnologies.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.YTExecutionResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class ODDLStatement extends OStatement {

  public ODDLStatement(int id) {
    super(id);
  }

  public ODDLStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OExecutionStream executeDDL(OCommandContext ctx);

  public YTResultSet execute(
      YTDatabaseSessionInternal db, Object[] args, OCommandContext parentCtx,
      boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public YTResultSet execute(
      YTDatabaseSessionInternal db, Map params, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return new YTExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new ODDLExecutionPlan(ctx, this);
  }
}
