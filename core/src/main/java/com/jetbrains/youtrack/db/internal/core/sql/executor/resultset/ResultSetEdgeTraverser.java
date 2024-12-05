package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.MatchEdgeTraverser;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public final class ResultSetEdgeTraverser implements ExecutionStream {

  private final MatchEdgeTraverser trav;
  private YTResult nextResult;

  public ResultSetEdgeTraverser(MatchEdgeTraverser trav) {
    this.trav = trav;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    fetchNext(ctx);
    return nextResult != null;
  }

  @Override
  public YTResult next(CommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }
    YTResult result = nextResult;
    ctx.setVariable("$matched", result);
    nextResult = null;
    return result;
  }

  @Override
  public void close(CommandContext ctx) {
  }

  private void fetchNext(CommandContext ctx) {
    if (nextResult == null) {
      while (trav.hasNext(ctx)) {
        nextResult = trav.next(ctx);
        if (nextResult != null) {
          break;
        }
      }
    }
  }
}
