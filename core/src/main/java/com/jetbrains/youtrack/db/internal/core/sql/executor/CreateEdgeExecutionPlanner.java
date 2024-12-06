package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBatch;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCreateEdgeStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ExecutionPlanCache;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInputParameter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInsertBody;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInsertSetExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLJson;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUpdateItem;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CreateEdgeExecutionPlanner {

  private final SQLCreateEdgeStatement statement;
  protected SQLIdentifier targetClass;
  protected SQLIdentifier targetClusterName;
  protected SQLExpression leftExpression;
  protected SQLExpression rightExpression;

  protected boolean upsert = false;

  protected SQLInsertBody body;
  protected Number retry;
  protected Number wait;
  protected SQLBatch batch;

  public CreateEdgeExecutionPlanner(SQLCreateEdgeStatement statement) {
    this.statement = statement;
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.leftExpression =
        statement.getLeftExpression() == null ? null : statement.getLeftExpression().copy();
    this.rightExpression =
        statement.getRightExpression() == null ? null : statement.getRightExpression().copy();
    this.upsert = statement.isUpsert();
    this.body = statement.getBody() == null ? null : statement.getBody().copy();
    this.retry = statement.getRetry();
    this.wait = statement.getWait();
    this.batch = statement.getBatch() == null ? null : statement.getBatch().copy();
  }

  public InsertExecutionPlan createExecutionPlan(
      CommandContext ctx, boolean enableProfiling, boolean useCache) {
    DatabaseSessionInternal db = ctx.getDatabase();
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached(db)) {
      ExecutionPlan plan = ExecutionPlanCache.get(statement.getOriginalStatement(), ctx, db);
      if (plan != null) {
        return (InsertExecutionPlan) plan;
      }
    }

    long planningStart = System.currentTimeMillis();

    if (targetClass == null) {
      if (targetClusterName == null) {
        targetClass = new SQLIdentifier("E");
      } else {
        SchemaClass clazz =
            db.getMetadata()
                .getImmutableSchemaSnapshot()
                .getClassByClusterId(db.getClusterIdByName(targetClusterName.getStringValue()));
        if (clazz != null) {
          targetClass = new SQLIdentifier(clazz.getName());
        } else {
          targetClass = new SQLIdentifier("E");
        }
      }
    }

    InsertExecutionPlan result = new InsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);

    handleGlobalLet(
        result,
        new SQLIdentifier("$__ORIENT_CREATE_EDGE_fromV"),
        leftExpression,
        ctx,
        enableProfiling);
    handleGlobalLet(
        result,
        new SQLIdentifier("$__ORIENT_CREATE_EDGE_toV"),
        rightExpression,
        ctx,
        enableProfiling);

    String uniqueIndexName = null;
    if (upsert) {
      SchemaClass clazz =
          ctx.getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(targetClass.getStringValue());
      if (clazz == null) {
        throw new CommandExecutionException(
            "Class " + targetClass + " not found in the db schema");
      }
      uniqueIndexName =
          clazz.getIndexes(db).stream()
              .filter(Index::isUnique)
              .filter(
                  x ->
                      x.getDefinition().getFields().size() == 2
                          && x.getDefinition().getFields().contains("out")
                          && x.getDefinition().getFields().contains("in"))
              .map(Index::getName)
              .findFirst()
              .orElse(null);

      if (uniqueIndexName == null) {
        throw new CommandExecutionException(
            "Cannot perform an UPSERT on "
                + targetClass
                + " edge class: no unique index present on out/in");
      }
    }

    result.chain(
        new CreateEdgesStep(
            targetClass,
            targetClusterName,
            uniqueIndexName,
            new SQLIdentifier("$__ORIENT_CREATE_EDGE_fromV"),
            new SQLIdentifier("$__ORIENT_CREATE_EDGE_toV"),
            wait,
            retry,
            batch,
            ctx,
            enableProfiling));

    handleSetFields(result, body, ctx, enableProfiling);
    handleSave(result, targetClusterName, ctx, enableProfiling);
    // TODO implement batch, wait and retry

    if (useCache
        && !enableProfiling
        && statement.executinPlanCanBeCached(db)
        && result.canBeCached()
        && ExecutionPlanCache.getLastInvalidation(db) < planningStart) {
      ExecutionPlanCache.put(statement.getOriginalStatement(), result, ctx.getDatabase());
    }

    return result;
  }

  private void handleGlobalLet(
      InsertExecutionPlan result,
      SQLIdentifier name,
      SQLExpression expression,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
  }

  private void handleCheckType(
      InsertExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(
          new CheckClassTypeStep(targetClass.getStringValue(), "E", ctx, profilingEnabled));
    }
  }

  private void handleSave(
      InsertExecutionPlan result,
      SQLIdentifier targetClusterName,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
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
}
