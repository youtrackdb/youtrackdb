package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.OExecutionThreadLocal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandInterruptedException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class InterruptResultSet implements ExecutionStream {

  private final ExecutionStream source;

  public InterruptResultSet(ExecutionStream source) {
    this.source = source;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new YTCommandInterruptedException("The command has been interrupted");
    }
    return source.hasNext(ctx);
  }

  @Override
  public YTResult next(CommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new YTCommandInterruptedException("The command has been interrupted");
    }
    return source.next(ctx);
  }

  @Override
  public void close(CommandContext ctx) {
    source.close(ctx);
  }
}
