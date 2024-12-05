package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultInternal;
import java.util.Iterator;

public class OIteratorExecutionStream implements OExecutionStream {

  private final Iterator<?> iterator;

  public OIteratorExecutionStream(Iterator<?> iter) {
    this.iterator = iter;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public YTResult next(OCommandContext ctx) {
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
  public void close(OCommandContext ctx) {
  }
}
