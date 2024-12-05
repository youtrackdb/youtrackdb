package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OProduceExecutionStream;

/**
 *
 */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private final int size;

  public EmptyDataGeneratorStep(int size, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.size = size;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(EmptyDataGeneratorStep::create).limit(size);
  }

  private static YTResult create(OCommandContext ctx) {
    YTResultInternal result = new YTResultInternal(ctx.getDatabase());
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
