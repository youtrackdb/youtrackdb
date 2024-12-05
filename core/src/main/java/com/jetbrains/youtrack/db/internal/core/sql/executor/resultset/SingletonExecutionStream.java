package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class SingletonExecutionStream implements ExecutionStream {

  private boolean executed = false;
  private final YTResult result;

  public SingletonExecutionStream(YTResult result) {
    this.result = result;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    return !executed;
  }

  @Override
  public YTResult next(CommandContext ctx) {
    if (executed) {
      throw new IllegalStateException();
    }
    executed = true;
    return result;
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
