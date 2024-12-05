package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

public class ProduceExecutionStream implements ExecutionStream {

  private final OProduceResult producer;

  public ProduceExecutionStream(OProduceResult producer) {
    if (producer == null) {
      throw new NullPointerException();
    }
    this.producer = producer;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    return true;
  }

  @Override
  public YTResult next(CommandContext ctx) {
    return producer.produce(ctx);
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
