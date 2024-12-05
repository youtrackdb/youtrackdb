package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBatch;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OMoveVertexStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OUpdateOperations;

/**
 *
 */
public class OMoveVertexExecutionPlanner {

  private final OFromItem source;
  private final OIdentifier targetClass;
  private final OCluster targetCluster;
  private final OUpdateOperations updateOperations;
  private final OBatch batch;

  public OMoveVertexExecutionPlanner(OMoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetClass = oStatement.getTargetClass();
    this.targetCluster = oStatement.getTargetCluster();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public OUpdateExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan(ctx);

    handleSource(result, ctx, this.source, enableProfiling);
    convertToModifiableResult(result, ctx, enableProfiling);
    handleTarget(result, targetClass, targetCluster, ctx, enableProfiling);
    handleOperations(result, ctx, this.updateOperations, enableProfiling);
    handleBatch(result, ctx, this.batch, enableProfiling);
    handleSave(result, ctx, enableProfiling);
    return result;
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      OIdentifier targetClass,
      OCluster targetCluster,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new MoveVertexStep(targetClass, targetCluster, ctx, profilingEnabled));
  }

  private void handleBatch(
      OUpdateExecutionPlan result, CommandContext ctx, OBatch batch, boolean profilingEnabled) {
    if (batch != null) {
      result.chain(new BatchStep(batch, ctx, profilingEnabled));
    }
  }

  /**
   * add a step that transforms a normal YTResult in a specific object that under setProperty()
   * updates the actual YTIdentifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(
      OUpdateExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new ConvertToUpdatableResultStep(ctx, profilingEnabled));
  }

  private void handleSave(
      OUpdateExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, profilingEnabled));
  }

  private void handleOperations(
      OUpdateExecutionPlan plan,
      CommandContext ctx,
      OUpdateOperations op,
      boolean profilingEnabled) {
    if (op != null) {
      switch (op.getType()) {
        case OUpdateOperations.TYPE_SET:
          plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx, profilingEnabled));
          break;
        case OUpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx, profilingEnabled));
          break;
        case OUpdateOperations.TYPE_MERGE:
          plan.chain(new UpdateMergeStep(op.getJson(), ctx, profilingEnabled));
          break;
        case OUpdateOperations.TYPE_CONTENT:
          plan.chain(new UpdateContentStep(op.getJson(), ctx, profilingEnabled));
          break;
        case OUpdateOperations.TYPE_PUT:
        case OUpdateOperations.TYPE_INCREMENT:
        case OUpdateOperations.TYPE_ADD:
          throw new YTCommandExecutionException(
              "Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(
      OUpdateExecutionPlan result,
      CommandContext ctx,
      OFromItem source,
      boolean profilingEnabled) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(new OFromClause(-1));
    sourceStatement.getTarget().setItem(source);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(
            planner.createExecutionPlan(ctx, profilingEnabled, false), ctx, ctx, profilingEnabled));
  }
}
