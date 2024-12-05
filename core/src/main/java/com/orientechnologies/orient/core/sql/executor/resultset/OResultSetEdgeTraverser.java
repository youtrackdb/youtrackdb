package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.MatchEdgeTraverser;
import com.orientechnologies.orient.core.sql.executor.YTResult;

public final class OResultSetEdgeTraverser implements OExecutionStream {

  private final MatchEdgeTraverser trav;
  private YTResult nextResult;

  public OResultSetEdgeTraverser(MatchEdgeTraverser trav) {
    this.trav = trav;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    fetchNext(ctx);
    return nextResult != null;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }
    YTResult result = nextResult;
    ctx.setVariable("$matched", result);
    nextResult = null;
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
  }

  private void fetchNext(OCommandContext ctx) {
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
