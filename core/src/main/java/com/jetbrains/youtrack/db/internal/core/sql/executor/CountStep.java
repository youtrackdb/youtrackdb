package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 * Counts the records from the previous steps. Returns a record with a single property, called
 * "count" containing the count of records received from pervious steps
 */
public class CountStep extends AbstractExecutionStep {

  /**
   * @param ctx              the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    ExecutionStream prevResult = prev.start(ctx);
    long count = 0;
    while (prevResult.hasNext(ctx)) {
      count++;
      prevResult.next(ctx);
    }
    prevResult.close(ctx);
    YTResultInternal resultRecord = new YTResultInternal(ctx.getDatabase());
    resultRecord.setProperty("count", count);
    return ExecutionStream.singleton(resultRecord);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new CountStep(ctx, profilingEnabled);
  }
}
