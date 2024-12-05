package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.OExecutionThreadLocal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandInterruptedException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.AbstractExecutionStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.EmptyStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OInternalExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OScriptExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.List;

public class WhileStep extends AbstractExecutionStep {

  private final SQLBooleanExpression condition;
  private final List<SQLStatement> statements;

  public WhileStep(
      SQLBooleanExpression condition,
      List<SQLStatement> statements,
      CommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.condition = condition;
    this.statements = statements;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    var db = ctx.getDatabase();
    while (condition.evaluate(new YTResultInternal(db), ctx)) {
      if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
        throw new YTCommandInterruptedException("The command has been interrupted");
      }

      OScriptExecutionPlan plan = initPlan(ctx);
      ExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        return result.start(ctx);
      }
    }
    return new EmptyStep(ctx, false).start(ctx);
  }

  public OScriptExecutionPlan initPlan(CommandContext ctx) {
    BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (SQLStatement stm : statements) {
      if (stm.originalStatement == null) {
        stm.originalStatement = stm.toString();
      }
      OInternalExecutionPlan subPlan;
      if (stm.originalStatement.contains("?")) {
        // cannot cache execution plans with positional parameters inside scripts
        subPlan = stm.createExecutionPlanNoCache(subCtx1, profilingEnabled);
      } else {
        subPlan = stm.createExecutionPlan(subCtx1, profilingEnabled);
      }
      plan.chain(subPlan, profilingEnabled);
    }
    return plan;
  }

  public boolean containsReturn() {
    for (SQLStatement stm : this.statements) {
      if (stm instanceof SQLReturnStatement) {
        return true;
      }
      if (stm instanceof SQLForEachBlock && ((SQLForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof SQLIfStatement && ((SQLIfStatement) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof SQLWhileBlock && ((SQLWhileBlock) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
