package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class EmptyExecutionStream implements ExecutionStream {

  protected static final ExecutionStream EMPTY = new EmptyExecutionStream();

  @Override
  public boolean hasNext(CommandContext ctx) {
    return false;
  }

  @Override
  public YTResult next(CommandContext ctx) {
    throw new IllegalStateException();
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
