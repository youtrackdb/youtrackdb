package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import java.util.Iterator;

public class ResultIteratorExecutionStream implements ExecutionStream {

  private final Iterator<YTResult> iterator;

  public ResultIteratorExecutionStream(Iterator<YTResult> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public YTResult next(CommandContext ctx) {
    return iterator.next();
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
