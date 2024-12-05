package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;

public class OFilterExecutionStream implements OExecutionStream {

  private final OExecutionStream prevResult;
  private final OFilterResult filter;
  private YTResult nextItem = null;

  public OFilterExecutionStream(OExecutionStream resultSet, OFilterResult filter) {
    super();
    this.prevResult = resultSet;
    this.filter = filter;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (nextItem == null) {
      fetchNextItem(ctx);
    }

    return nextItem != null;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (nextItem == null) {
      fetchNextItem(ctx);
    }
    if (nextItem == null) {
      throw new IllegalStateException();
    }
    YTResult result = nextItem;
    nextItem = null;
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
    this.prevResult.close(ctx);
  }

  private void fetchNextItem(OCommandContext ctx) {
    while (prevResult.hasNext(ctx)) {
      nextItem = prevResult.next(ctx);
      nextItem = filter.filterMap(nextItem, ctx);
      if (nextItem != null) {
        break;
      }
    }
  }
}
