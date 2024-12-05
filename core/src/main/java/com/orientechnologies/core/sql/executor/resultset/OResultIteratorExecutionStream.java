package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;
import java.util.Iterator;

public class OResultIteratorExecutionStream implements OExecutionStream {

  private final Iterator<YTResult> iterator;

  public OResultIteratorExecutionStream(Iterator<YTResult> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    return iterator.next();
  }

  @Override
  public void close(OCommandContext ctx) {
  }
}
