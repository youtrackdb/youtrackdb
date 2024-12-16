package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.Iterator;

public class IteratorExecutionStream implements ExecutionStream {

  private final Iterator<?> iterator;

  public IteratorExecutionStream(Iterator<?> iter) {
    this.iterator = iter;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public Result next(CommandContext ctx) {
    Object val = iterator.next();
    if (val instanceof Result) {
      return (Result) val;
    }

    ResultInternal result;
    var db = ctx.getDatabase();
    if (val instanceof Identifiable) {
      result = new ResultInternal(db, (Identifiable) val);
    } else {
      result = new ResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
