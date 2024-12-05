package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.YTResult;

public class OLimitedExecutionStream implements OExecutionStream {

  private final OExecutionStream upstream;
  private final long limit;
  private long count = 0;

  public OLimitedExecutionStream(OExecutionStream upstream, long limit) {
    this.upstream = upstream;
    this.limit = limit;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (count >= limit) {
      return false;
    } else {
      return upstream.hasNext(ctx);
    }
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (count >= limit) {
      throw new IllegalStateException();
    } else {
      YTResult read = upstream.next(ctx);
      this.count += 1;
      return read;
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    upstream.close(ctx);
  }
}
