package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OReturnStatement;

/**
 * <p>This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {

  protected final OInternalExecutionPlan plan;

  public ScriptLineStep(
      OInternalExecutionPlan nextPlan, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.plan = nextPlan;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (plan instanceof OInsertExecutionPlan) {
      ((OInsertExecutionPlan) plan).executeInternal();
    } else if (plan instanceof ODeleteExecutionPlan) {
      ((ODeleteExecutionPlan) plan).executeInternal();
    } else if (plan instanceof OUpdateExecutionPlan) {
      ((OUpdateExecutionPlan) plan).executeInternal();
    } else if (plan instanceof ODDLExecutionPlan) {
      ((ODDLExecutionPlan) plan).executeInternal((BasicCommandContext) ctx);
    } else if (plan instanceof OSingleOpExecutionPlan) {
      ((OSingleOpExecutionPlan) plan).executeInternal((BasicCommandContext) ctx);
    }
    return plan.start();
  }

  public boolean containsReturn() {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).containsReturn();
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return true;
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      if (((OIfExecutionPlan) plan).containsReturn()) {
        return true;
      }
    }

    if (plan instanceof OForEachExecutionPlan) {
      return ((OForEachExecutionPlan) plan).containsReturn();
    }
    return false;
  }

  public ExecutionStepInternal executeUntilReturn(CommandContext ctx) {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).executeUntilReturn();
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return new ReturnStep(((OSingleOpExecutionPlan) plan).statement, ctx, profilingEnabled);
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      return ((OIfExecutionPlan) plan).executeUntilReturn();
    }
    throw new IllegalStateException();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (plan == null) {
      return "Script Line";
    }
    return plan.prettyPrint(depth, indent);
  }
}
