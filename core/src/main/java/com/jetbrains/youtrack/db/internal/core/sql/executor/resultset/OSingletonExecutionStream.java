package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class OSingletonExecutionStream implements OExecutionStream {

  private boolean executed = false;
  private final YTResult result;

  public OSingletonExecutionStream(YTResult result) {
    this.result = result;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return !executed;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (executed) {
      throw new IllegalStateException();
    }
    executed = true;
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
  }
}
