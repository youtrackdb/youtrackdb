package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public final class OFlatMapExecutionStream implements OExecutionStream {

  private final OExecutionStream base;
  private final OMapExecutionStream map;
  private OExecutionStream currentResultSet;

  public OFlatMapExecutionStream(OExecutionStream base, OMapExecutionStream map) {
    this.base = base;
    this.map = map;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    while (currentResultSet == null || !currentResultSet.hasNext(ctx)) {
      if (currentResultSet != null) {
        currentResultSet.close(ctx);
      }
      if (!base.hasNext(ctx)) {
        return false;
      }
      currentResultSet = map.flatMap(base.next(ctx), ctx);
    }
    return true;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }
    return currentResultSet.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    if (currentResultSet != null) {
      currentResultSet.close(ctx);
    }
    base.close(ctx);
  }
}
