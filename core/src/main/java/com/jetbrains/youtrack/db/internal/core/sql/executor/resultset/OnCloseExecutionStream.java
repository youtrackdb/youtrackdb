package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class OnCloseExecutionStream implements OExecutionStream {

  private final OExecutionStream source;
  private final OnClose onClose;

  public OnCloseExecutionStream(OExecutionStream source, OnClose onClose) {
    this.source = source;
    this.onClose = onClose;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return source.hasNext(ctx);
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    return source.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    onClose.close(ctx);
    source.close(ctx);
  }
}
