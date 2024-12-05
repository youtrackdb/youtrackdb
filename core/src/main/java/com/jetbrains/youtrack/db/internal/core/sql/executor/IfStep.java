package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIfStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OReturnStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatement;
import java.util.List;

/**
 *
 */
public class IfStep extends AbstractExecutionStep {

  protected OBooleanExpression condition;
  public List<OStatement> positiveStatements;
  public List<OStatement> negativeStatements;

  public IfStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    OScriptExecutionPlan plan = producePlan(ctx);
    if (plan != null) {
      return plan.start();
    } else {
      return ExecutionStream.empty();
    }
  }

  public OScriptExecutionPlan producePlan(CommandContext ctx) {
    if (condition.evaluate((YTResult) null, ctx)) {
      OScriptExecutionPlan positivePlan = initPositivePlan(ctx);
      return positivePlan;
    } else {
      OScriptExecutionPlan negativePlan = initNegativePlan(ctx);
      return negativePlan;
    }
  }

  public OScriptExecutionPlan initPositivePlan(CommandContext ctx) {
    BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan positivePlan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : positiveStatements) {
      positivePlan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return positivePlan;
  }

  public OScriptExecutionPlan initNegativePlan(CommandContext ctx) {
    if (negativeStatements != null) {
      if (negativeStatements.size() > 0) {
        BasicCommandContext subCtx2 = new BasicCommandContext();
        subCtx2.setParent(ctx);
        OScriptExecutionPlan negativePlan = new OScriptExecutionPlan(subCtx2);
        for (OStatement stm : negativeStatements) {
          negativePlan.chain(stm.createExecutionPlan(subCtx2, profilingEnabled), profilingEnabled);
        }
        return negativePlan;
      }
    }
    return null;
  }

  public OBooleanExpression getCondition() {
    return condition;
  }

  public void setCondition(OBooleanExpression condition) {
    this.condition = condition;
  }

  public boolean containsReturn() {
    if (positiveStatements != null) {
      for (OStatement stm : positiveStatements) {
        if (containsReturn(stm)) {
          return true;
        }
      }
    }
    if (negativeStatements != null) {
      for (OStatement stm : negativeStatements) {
        if (containsReturn(stm)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean containsReturn(OStatement stm) {
    if (stm instanceof OReturnStatement) {
      return true;
    }
    if (stm instanceof OIfStatement) {
      for (OStatement o : ((OIfStatement) stm).getStatements()) {
        if (containsReturn(o)) {
          return true;
        }
      }
    }
    return false;
  }
}
