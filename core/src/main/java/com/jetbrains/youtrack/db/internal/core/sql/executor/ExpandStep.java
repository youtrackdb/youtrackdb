package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Iterator;

/**
 * Expands a result-set. The pre-requisite is that the input element contains only one field (no
 * matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {

  public ExpandStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev == null) {
      throw new CommandExecutionException("Cannot expand without a target");
    }
    var resultSet = prev.start(ctx);
    return resultSet.flatMap(ExpandStep::nextResults);
  }

  private static ExecutionStream nextResults(Result nextAggregateItem, CommandContext ctx) {
    if (nextAggregateItem.getPropertyNames().isEmpty()) {
      return ExecutionStream.empty();
    }
    if (nextAggregateItem.getPropertyNames().size() > 1) {
      throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
    }

    var propName = nextAggregateItem.getPropertyNames().iterator().next();
    var projValue = nextAggregateItem.getProperty(propName);
    var db = ctx.getDatabase();
    switch (projValue) {
      case null -> {
        return ExecutionStream.empty();
      }
      case Identifiable identifiable -> {
        DBRecord rec;
        try {
          rec = ((Identifiable) projValue).getRecord(db);
        } catch (RecordNotFoundException rnf) {
          return ExecutionStream.empty();
        }

        var res = new ResultInternal(ctx.getDatabase(), rec);
        return ExecutionStream.singleton(res);
      }
      case Result result -> {
        return ExecutionStream.singleton(result);
      }
      case Iterator iterator -> {
        //noinspection unchecked
        return ExecutionStream.iterator((Iterator<Object>) projValue);
        //noinspection unchecked
      }
      case Iterable iterable -> {
        //noinspection unchecked
        return ExecutionStream.iterator(((Iterable<Object>) projValue).iterator());
        //noinspection unchecked
      }
      default -> {
        return ExecutionStream.empty();
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
    var result = spaces + "+ EXPAND";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
