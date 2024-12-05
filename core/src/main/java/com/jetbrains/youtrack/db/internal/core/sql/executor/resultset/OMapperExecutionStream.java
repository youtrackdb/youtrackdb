package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class OMapperExecutionStream implements OExecutionStream {

  private final OExecutionStream upstream;
  private final OResultMapper mapper;

  public OMapperExecutionStream(OExecutionStream upstream, OResultMapper mapper) {
    if (upstream == null || mapper == null) {
      throw new NullPointerException();
    }
    this.upstream = upstream;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return upstream.hasNext(ctx);
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    return this.mapper.map(upstream.next(ctx), ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    this.upstream.close(ctx);
  }
}
