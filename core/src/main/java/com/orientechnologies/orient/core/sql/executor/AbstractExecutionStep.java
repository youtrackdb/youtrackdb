package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OStepStats;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.text.DecimalFormat;
import javax.annotation.Nullable;

/**
 *
 */
public abstract class AbstractExecutionStep implements OExecutionStepInternal {

  final OCommandContext ctx;
  @Nullable
  protected OExecutionStepInternal prev = null;
  @Nullable
  OExecutionStepInternal next = null;
  protected boolean profilingEnabled;

  public AbstractExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    this.ctx = ctx;
    this.profilingEnabled = profilingEnabled;
  }

  @Override
  public void setPrevious(OExecutionStepInternal step) {
    this.prev = step;
  }

  @Override
  public void setNext(@Nullable OExecutionStepInternal step) {
    this.next = step;
  }

  @Override
  public void sendTimeout() {
    if (prev != null) {
      prev.sendTimeout();
    }
  }

  private boolean alreadyClosed = false;

  @Override
  public void close() {
    if (alreadyClosed) {
      return;
    }
    alreadyClosed = true;

    if (prev != null) {
      prev.close();
    }
  }

  public boolean isProfilingEnabled() {
    return profilingEnabled;
  }

  public void setProfilingEnabled(boolean profilingEnabled) {
    this.profilingEnabled = profilingEnabled;
  }

  public OExecutionStream start(OCommandContext ctx) throws OTimeoutException {
    if (profilingEnabled) {
      ctx.startProfiling(this);
      try {
        return internalStart(ctx).profile(this);
      } finally {
        ctx.endProfiling(this);
      }
    } else {
      return internalStart(ctx);
    }
  }

  protected abstract OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException;

  @Override
  public long getCost() {
    OStepStats stats = this.ctx.getStats(this);
    if (stats != null) {
      return stats.getCost();
    } else {
      return OExecutionStepInternal.super.getCost();
    }
  }

  protected String getCostFormatted() {
    return new DecimalFormat().format(getCost() / 1000) + "μs";
  }
}
