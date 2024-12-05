package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.Record;
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
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException("Cannot expand without a target");
    }
    ExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(ExpandStep::nextResults);
  }

  private static ExecutionStream nextResults(YTResult nextAggregateItem, CommandContext ctx) {
    if (nextAggregateItem.getPropertyNames().isEmpty()) {
      return ExecutionStream.empty();
    }
    if (nextAggregateItem.getPropertyNames().size() > 1) {
      throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
    }

    String propName = nextAggregateItem.getPropertyNames().iterator().next();
    Object projValue = nextAggregateItem.getProperty(propName);
    if (projValue == null) {
      return ExecutionStream.empty();
    }
    if (projValue instanceof YTIdentifiable) {
      Record rec;
      try {
        rec = ((YTIdentifiable) projValue).getRecord();
      } catch (YTRecordNotFoundException rnf) {
        return ExecutionStream.empty();
      }

      YTResultInternal res = new YTResultInternal(ctx.getDatabase(), rec);
      return ExecutionStream.singleton(res);
    } else if (projValue instanceof YTResult) {
      return ExecutionStream.singleton((YTResult) projValue);
    } else if (projValue instanceof Iterator) {
      //noinspection unchecked
      return ExecutionStream.iterator((Iterator<Object>) projValue);
    } else if (projValue instanceof Iterable) {
      //noinspection unchecked
      return ExecutionStream.iterator(((Iterable<Object>) projValue).iterator());
    } else {
      return ExecutionStream.empty();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXPAND";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
