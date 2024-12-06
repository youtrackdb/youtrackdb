package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import java.util.Iterator;

public class ResultIteratorExecutionStream implements ExecutionStream {

  private final Iterator<Result> iterator;

  public ResultIteratorExecutionStream(Iterator<Result> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public Result next(CommandContext ctx) {
    return iterator.next();
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
