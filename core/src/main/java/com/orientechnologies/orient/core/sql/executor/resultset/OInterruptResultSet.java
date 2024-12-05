package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.YTCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.YTResult;

public class OInterruptResultSet implements OExecutionStream {

  private final OExecutionStream source;

  public OInterruptResultSet(OExecutionStream source) {
    this.source = source;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new YTCommandInterruptedException("The command has been interrupted");
    }
    return source.hasNext(ctx);
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new YTCommandInterruptedException("The command has been interrupted");
    }
    return source.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    source.close(ctx);
  }
}
