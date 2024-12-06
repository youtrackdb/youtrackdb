package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.IndexAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIndexIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInputParameter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInsertBody;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInsertSetExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInsertStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLJson;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUpdateItem;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class InsertExecutionPlanner {

  protected SQLIdentifier targetClass;
  protected SQLIdentifier targetClusterName;
  protected SQLCluster targetCluster;
  protected SQLIndexIdentifier targetIndex;
  protected SQLInsertBody insertBody;
  protected SQLProjection returnStatement;
  protected SQLSelectStatement selectStatement;

  public InsertExecutionPlanner() {
  }

  public InsertExecutionPlanner(SQLInsertStatement statement) {
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster =
        statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    this.targetIndex =
        statement.getTargetIndex() == null ? null : statement.getTargetIndex().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement =
        statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement =
        statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public InsertExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    InsertExecutionPlan result = new InsertExecutionPlan(ctx);

    if (targetIndex != null) {
      IndexAbstract.manualIndexesWarning();
      result.chain(new InsertIntoIndexStep(targetIndex, insertBody, ctx, enableProfiling));
    } else {
      if (selectStatement != null) {
        handleInsertSelect(result, this.selectStatement, ctx, enableProfiling);
      } else {
        handleCreateRecord(result, this.insertBody, ctx, enableProfiling);
      }
      handleTargetClass(result, ctx, enableProfiling);
      handleSetFields(result, insertBody, ctx, enableProfiling);
      var database = ctx.getDatabase();
      if (targetCluster != null) {
        String name = targetCluster.getClusterName();
        if (name == null) {
          name = database.getClusterNameById(targetCluster.getClusterNumber());
        }
        handleSave(result, new SQLIdentifier(name), ctx, enableProfiling);
      } else {
        handleSave(result, targetClusterName, ctx, enableProfiling);
      }
      handleReturn(result, returnStatement, ctx, enableProfiling);
    }
    return result;
  }

  private void handleSave(
      InsertExecutionPlan result,
      SQLIdentifier targetClusterName,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleReturn(
      InsertExecutionPlan result,
      SQLProjection returnStatement,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (returnStatement != null) {
      result.chain(new ProjectionCalculationStep(returnStatement, ctx, profilingEnabled));
    }
  }

  private void handleSetFields(
      InsertExecutionPlan result,
      SQLInsertBody insertBody,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(
          new InsertValuesStep(
              insertBody.getIdentifierList(),
              insertBody.getValueExpressions(),
              ctx,
              profilingEnabled));
    } else if (insertBody.getContent() != null) {
      for (SQLJson json : insertBody.getContent()) {
        result.chain(new UpdateContentStep(json, ctx, profilingEnabled));
      }
    } else if (insertBody.getContentInputParam() != null) {
      for (SQLInputParameter inputParam : insertBody.getContentInputParam()) {
        result.chain(new UpdateContentStep(inputParam, ctx, profilingEnabled));
      }
    } else if (insertBody.getSetExpressions() != null) {
      List<SQLUpdateItem> items = new ArrayList<>();
      for (SQLInsertSetExpression exp : insertBody.getSetExpressions()) {
        SQLUpdateItem item = new SQLUpdateItem(-1);
        item.setOperator(SQLUpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(
      InsertExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    var database = ctx.getDatabase();
    Schema schema = database.getMetadata().getSchema();
    SQLIdentifier tc = null;
    if (targetClass != null) {
      tc = targetClass;
    } else if (targetCluster != null) {
      String name = targetCluster.getClusterName();
      if (name == null) {
        name = database.getClusterNameById(targetCluster.getClusterNumber());
      }
      SchemaClass targetClass = schema.getClassByClusterId(database.getClusterIdByName(name));
      if (targetClass != null) {
        tc = new SQLIdentifier(targetClass.getName());
      }
    } else if (this.targetClass == null) {

      SchemaClass targetClass =
          schema.getClassByClusterId(
              database.getClusterIdByName(targetClusterName.getStringValue()));
      if (targetClass != null) {
        tc = new SQLIdentifier(targetClass.getName());
      }
    }
    if (tc != null) {
      result.chain(new SetDocumentClassStep(tc, ctx, profilingEnabled));
    }
  }

  private void handleCreateRecord(
      InsertExecutionPlan result,
      SQLInsertBody body,
      CommandContext ctx,
      boolean profilingEnabled) {
    int tot = 1;

    if (body != null
        && body.getValueExpressions() != null
        && body.getValueExpressions().size() > 0) {
      tot = body.getValueExpressions().size();
    }
    if (body != null
        && body.getContentInputParam() != null
        && body.getContentInputParam().size() > 0) {
      tot = body.getContentInputParam().size();
      if (body != null && body.getContent() != null && body.getContent().size() > 0) {
        tot += body.getContent().size();
      }
    } else {
      if (body != null && body.getContent() != null && body.getContent().size() > 0) {
        tot = body.getContent().size();
      }
    }
    result.chain(new CreateRecordStep(ctx, tot, profilingEnabled));
  }

  private void handleInsertSelect(
      InsertExecutionPlan result,
      SQLSelectStatement selectStatement,
      CommandContext ctx,
      boolean profilingEnabled) {
    InternalExecutionPlan subPlan = selectStatement.createExecutionPlan(ctx, profilingEnabled);
    result.chain(new SubQueryStep(subPlan, ctx, ctx, profilingEnabled));
    result.chain(new CopyDocumentStep(ctx, profilingEnabled));
    result.chain(new RemoveEdgePointersStep(ctx, profilingEnabled));
  }
}
