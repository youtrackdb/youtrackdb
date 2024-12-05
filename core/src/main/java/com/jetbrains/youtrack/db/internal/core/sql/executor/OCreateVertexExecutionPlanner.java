package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OCreateVertexStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OCreateVertexExecutionPlanner extends OInsertExecutionPlanner {

  public OCreateVertexExecutionPlanner(OCreateVertexStatement statement) {
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster =
        statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    if (this.targetClass == null && this.targetCluster == null && this.targetClusterName == null) {
      this.targetClass = new OIdentifier("V");
    }
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement =
        statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
  }

  @Override
  public OInsertExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    OInsertExecutionPlan prev = super.createExecutionPlan(ctx, enableProfiling);
    List<ExecutionStep> steps = new ArrayList<>(prev.getSteps());
    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);
    for (ExecutionStep step : steps) {
      result.chain((ExecutionStepInternal) step);
    }
    return result;
  }

  private void handleCheckType(
      OInsertExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(
          new CheckClassTypeStep(targetClass.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetClusterName != null) {
      result.chain(
          new CheckClusterTypeStep(targetClusterName.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetCluster != null) {
      result.chain(new CheckClusterTypeStep(targetCluster, "V", ctx, profilingEnabled));
    }
  }
}
