package com.jetbrains.youtrack.db.internal.core.storage.cache.local;

public final class PeriodicFlushTask implements Runnable {

  /**
   * @param owowCache
   */
  private final OWOWCache owowCache;

  public PeriodicFlushTask(OWOWCache owowCache) {
    this.owowCache = owowCache;
  }

  @Override
  public void run() {
    this.owowCache.executePeriodicFlush(this);
  }
}
