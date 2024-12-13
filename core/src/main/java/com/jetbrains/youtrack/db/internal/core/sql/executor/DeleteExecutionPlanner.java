package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLDeleteStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIndexIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLWhereClause;
import java.util.List;

/**
 *
 */
public class DeleteExecutionPlanner {

  private final SQLFromClause fromClause;
  private final SQLWhereClause whereClause;
  private final boolean returnBefore;
  private final SQLLimit limit;
  private final boolean unsafe;

  public DeleteExecutionPlanner(SQLDeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public DeleteExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    DeleteExecutionPlan result = new DeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(
        result, fromClause.getItem().getIndex(), whereClause, ctx, enableProfiling)) {
      if (limit != null) {
        throw new CommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new CommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new CommandExecutionException(
            "Cannot apply a RETURN BEFORE on a delete from index");
      }

      handleReturn(result, ctx, this.returnBefore, enableProfiling);
    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause, enableProfiling);
      handleUnsafe(result, ctx, this.unsafe, enableProfiling);
      handleLimit(result, ctx, this.limit, enableProfiling);
      handleDelete(result, ctx, enableProfiling);
      handleReturn(result, ctx, this.returnBefore, enableProfiling);
    }
    return result;
  }

  private boolean handleIndexAsTarget(
      DeleteExecutionPlan result,
      SQLIndexIdentifier indexIdentifier,
      SQLWhereClause whereClause,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (indexIdentifier == null) {
      return false;
    }
    String indexName = indexIdentifier.getIndexName();
    final DatabaseSessionInternal database = ctx.getDatabase();
    Index index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }
    List<SQLAndBlock> flattenedWhereClause = whereClause == null ? null : whereClause.flatten();

    switch (indexIdentifier.getType()) {
      case INDEX:
        IndexAbstract.manualIndexesWarning();

        SQLBooleanExpression keyCondition = null;
        SQLBooleanExpression ridCondition = null;
        if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
          if (!index.supportsOrderedIterations()) {
            throw new CommandExecutionException(
                "Index " + indexName + " does not allow iteration without a condition");
          }
        } else if (flattenedWhereClause.size() > 1) {
          throw new CommandExecutionException(
              "Index queries with this kind of condition are not supported yet: " + whereClause);
        } else {
          SQLAndBlock andBlock = flattenedWhereClause.get(0);
          if (andBlock.getSubBlocks().size() == 1) {

            whereClause =
                null; // The WHERE clause won't be used anymore, the index does all the filtering
            flattenedWhereClause = null;
            keyCondition = getKeyCondition(andBlock);
            if (keyCondition == null) {
              throw new CommandExecutionException(
                  "Index queries with this kind of condition are not supported yet: "
                      + whereClause);
            }
          } else if (andBlock.getSubBlocks().size() == 2) {
            whereClause =
                null; // The WHERE clause won't be used anymore, the index does all the filtering
            flattenedWhereClause = null;
            keyCondition = getKeyCondition(andBlock);
            ridCondition = getRidCondition(andBlock);
            if (keyCondition == null || ridCondition == null) {
              throw new CommandExecutionException(
                  "Index queries with this kind of condition are not supported yet: "
                      + whereClause);
            }
          } else {
            throw new CommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        }
        result.chain(
            new DeleteFromIndexStep(
                index, keyCondition, null, ridCondition, ctx, profilingEnabled));
        if (ridCondition != null) {
          SQLWhereClause where = new SQLWhereClause(-1);
          where.setBaseExpression(ridCondition);
          result.chain(new FilterStep(where, ctx, -1, profilingEnabled));
        }
        return true;
      case VALUES:
      case VALUESASC:
        if (!index.supportsOrderedIterations()) {
          throw new CommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(
                new IndexSearchDescriptor(index), true, ctx, profilingEnabled));
        result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new CommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(
                new IndexSearchDescriptor(index), false, ctx, profilingEnabled));
        result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
        break;
    }
    return false;
  }

  private void handleDelete(
      DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleUnsafe(
      DeleteExecutionPlan result, CommandContext ctx, boolean unsafe, boolean profilingEnabled) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep(ctx, profilingEnabled));
    }
  }

  private void handleReturn(
      DeleteExecutionPlan result,
      CommandContext ctx,
      boolean returnBefore,
      boolean profilingEnabled) {
    if (!returnBefore) {
      result.chain(new CountStep(ctx, profilingEnabled));
    }
  }

  private void handleLimit(
      UpdateExecutionPlan plan, CommandContext ctx, SQLLimit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleTarget(
      UpdateExecutionPlan result,
      CommandContext ctx,
      SQLFromClause target,
      SQLWhereClause whereClause,
      boolean profilingEnabled) {
    SQLSelectStatement sourceStatement = new SQLSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(
            planner.createExecutionPlan(ctx, profilingEnabled, false), ctx, ctx, profilingEnabled));
  }

  private SQLBooleanExpression getKeyCondition(SQLAndBlock andBlock) {
    for (SQLBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private SQLBooleanExpression getRidCondition(SQLAndBlock andBlock) {
    for (SQLBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }
}
