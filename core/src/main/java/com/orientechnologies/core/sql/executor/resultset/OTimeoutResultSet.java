package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import java.util.concurrent.atomic.AtomicLong;

public final class OTimeoutResultSet implements OExecutionStream {

  private final AtomicLong totalTime = new AtomicLong(0);
  private final TimedOut timedout;
  private final OExecutionStream internal;
  private final long timeoutMillis;
  private boolean timedOut = false;

  public interface TimedOut {

    void timeout();
  }

  public OTimeoutResultSet(OExecutionStream internal, long timeoutMillis, TimedOut timedout) {
    this.internal = internal;
    this.timedout = timedout;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (timedOut) {
      return false;
    }
    long begin = System.nanoTime();

    try {
      return internal.hasNext(ctx);
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (totalTime.get() / 1_000_000 > timeoutMillis) {
      fail();
      if (timedOut) {
        return new YTResultInternal(ctx.getDatabase());
      }
    }
    long begin = System.nanoTime();
    try {
      return internal.next(ctx);
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    internal.close(ctx);
  }

  private void fail() {
    this.timedOut = true;
    this.timedout.timeout();
  }
}
