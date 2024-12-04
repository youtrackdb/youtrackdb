package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
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
  public OResult next(OCommandContext ctx) {
    Object val = iterator.next();
    if (val instanceof OResult) {
      return (OResult) val;
    }

    OResultInternal result;
    var db = ctx.getDatabase();
    if (val instanceof YTIdentifiable) {
      result = new OResultInternal(db, (YTIdentifiable) val);
    } else {
      result = new OResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
  }
}
