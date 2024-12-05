package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;

public class OProduceExecutionStream implements OExecutionStream {

  private final OProduceResult producer;

  public OProduceExecutionStream(OProduceResult producer) {
    if (producer == null) {
      throw new NullPointerException();
    }
    this.producer = producer;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return true;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    return producer.produce(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
  }
}
