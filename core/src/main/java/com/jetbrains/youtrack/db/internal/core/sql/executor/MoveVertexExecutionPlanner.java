package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBatch;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLMoveVertexStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUpdateOperations;

/**
 *
 */
public class MoveVertexExecutionPlanner {

  private final SQLFromItem source;
  private final SQLIdentifier targetClass;
  private final SQLCluster targetCluster;
  private final SQLUpdateOperations updateOperations;
  private final SQLBatch batch;

  public MoveVertexExecutionPlanner(SQLMoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetClass = oStatement.getTargetClass();
    this.targetCluster = oStatement.getTargetCluster();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public UpdateExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    var result = new UpdateExecutionPlan(ctx);

    handleSource(result, ctx, this.source, enableProfiling);
    convertToModifiableResult(result, ctx, enableProfiling);
    handleTarget(result, targetClass, targetCluster, ctx, enableProfiling);
    handleOperations(result, ctx, this.updateOperations, enableProfiling);
    handleBatch(result, ctx, this.batch, enableProfiling);
    handleSave(result, ctx, enableProfiling);
    return result;
  }

  private void handleTarget(
      UpdateExecutionPlan result,
      SQLIdentifier targetClass,
      SQLCluster targetCluster,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new MoveVertexStep(targetClass, targetCluster, ctx, profilingEnabled));
  }

  private void handleBatch(
      UpdateExecutionPlan result, CommandContext ctx, SQLBatch batch, boolean profilingEnabled) {
    if (batch != null) {
      result.chain(new BatchStep(batch, ctx, profilingEnabled));
    }
  }

  /**
   * add a step that transforms a normal Result in a specific object that under setProperty()
   * updates the actual Identifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(
      UpdateExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new ConvertToUpdatableResultStep(ctx, profilingEnabled));
  }

  private void handleSave(
      UpdateExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, profilingEnabled));
  }

  private void handleOperations(
      UpdateExecutionPlan plan,
      CommandContext ctx,
      SQLUpdateOperations op,
      boolean profilingEnabled) {
    if (op != null) {
      switch (op.getType()) {
        case SQLUpdateOperations.TYPE_SET:
          plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx, profilingEnabled));
          break;
        case SQLUpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx, profilingEnabled));
          break;
        case SQLUpdateOperations.TYPE_MERGE:
          plan.chain(new UpdateMergeStep(op.getJson(), ctx, profilingEnabled));
          break;
        case SQLUpdateOperations.TYPE_CONTENT:
          plan.chain(new UpdateContentStep(op.getJson(), ctx, profilingEnabled));
          break;
        case SQLUpdateOperations.TYPE_PUT:
        case SQLUpdateOperations.TYPE_INCREMENT:
        case SQLUpdateOperations.TYPE_ADD:
          throw new CommandExecutionException(
              "Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(
      UpdateExecutionPlan result,
      CommandContext ctx,
      SQLFromItem source,
      boolean profilingEnabled) {
    var sourceStatement = new SQLSelectStatement(-1);
    sourceStatement.setTarget(new SQLFromClause(-1));
    sourceStatement.getTarget().setItem(source);
    var planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(
            planner.createExecutionPlan(ctx, profilingEnabled, false), ctx, ctx, profilingEnabled));
  }
}
