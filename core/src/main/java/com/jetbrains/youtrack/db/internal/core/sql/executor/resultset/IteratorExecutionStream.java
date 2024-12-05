package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
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
  public YTResult next(CommandContext ctx) {
    Object val = iterator.next();
    if (val instanceof YTResult) {
      return (YTResult) val;
    }

    YTResultInternal result;
    var db = ctx.getDatabase();
    if (val instanceof YTIdentifiable) {
      result = new YTResultInternal(db, (YTIdentifiable) val);
    } else {
      result = new YTResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close(CommandContext ctx) {
  }
}
