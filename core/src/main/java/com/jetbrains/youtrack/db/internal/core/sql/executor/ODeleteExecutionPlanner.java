package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexAbstract;
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
public class ODeleteExecutionPlanner {

  private final SQLFromClause fromClause;
  private final SQLWhereClause whereClause;
  private final boolean returnBefore;
  private final SQLLimit limit;
  private final boolean unsafe;

  public ODeleteExecutionPlanner(SQLDeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public ODeleteExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(
        result, fromClause.getItem().getIndex(), whereClause, ctx, enableProfiling)) {
      if (limit != null) {
        throw new YTCommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new YTCommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new YTCommandExecutionException(
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
      ODeleteExecutionPlan result,
      SQLIndexIdentifier indexIdentifier,
      SQLWhereClause whereClause,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (indexIdentifier == null) {
      return false;
    }
    String indexName = indexIdentifier.getIndexName();
    final YTDatabaseSessionInternal database = ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      throw new YTCommandExecutionException("Index not found: " + indexName);
    }
    List<SQLAndBlock> flattenedWhereClause = whereClause == null ? null : whereClause.flatten();

    switch (indexIdentifier.getType()) {
      case INDEX:
        OIndexAbstract.manualIndexesWarning();

        SQLBooleanExpression keyCondition = null;
        SQLBooleanExpression ridCondition = null;
        if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
          if (!index.supportsOrderedIterations()) {
            throw new YTCommandExecutionException(
                "Index " + indexName + " does not allow iteration without a condition");
          }
        } else if (flattenedWhereClause.size() > 1) {
          throw new YTCommandExecutionException(
              "Index queries with this kind of condition are not supported yet: " + whereClause);
        } else {
          SQLAndBlock andBlock = flattenedWhereClause.get(0);
          if (andBlock.getSubBlocks().size() == 1) {

            whereClause =
                null; // The WHERE clause won't be used anymore, the index does all the filtering
            flattenedWhereClause = null;
            keyCondition = getKeyCondition(andBlock);
            if (keyCondition == null) {
              throw new YTCommandExecutionException(
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
              throw new YTCommandExecutionException(
                  "Index queries with this kind of condition are not supported yet: "
                      + whereClause);
            }
          } else {
            throw new YTCommandExecutionException(
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
          throw new YTCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(
                new IndexSearchDescriptor(index), true, ctx, profilingEnabled));
        result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new YTCommandExecutionException(
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
      ODeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleUnsafe(
      ODeleteExecutionPlan result, CommandContext ctx, boolean unsafe, boolean profilingEnabled) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep(ctx, profilingEnabled));
    }
  }

  private void handleReturn(
      ODeleteExecutionPlan result,
      CommandContext ctx,
      boolean returnBefore,
      boolean profilingEnabled) {
    if (!returnBefore) {
      result.chain(new CountStep(ctx, profilingEnabled));
    }
  }

  private void handleLimit(
      OUpdateExecutionPlan plan, CommandContext ctx, SQLLimit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      CommandContext ctx,
      SQLFromClause target,
      SQLWhereClause whereClause,
      boolean profilingEnabled) {
    SQLSelectStatement sourceStatement = new SQLSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
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
