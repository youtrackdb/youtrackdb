package com.jetbrains.youtrack.db.internal.core.storage.disk;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;

public class OPeriodicFuzzyCheckpoint implements Runnable {

  /**
   *
   */
  private final LocalPaginatedStorage storage;

  /**
   * @param storage
   */
  public OPeriodicFuzzyCheckpoint(LocalPaginatedStorage storage) {
    this.storage = storage;
  }

  @Override
  public final void run() {
    try {
      storage.makeFuzzyCheckpoint();
    } catch (final RuntimeException e) {
      LogManager.instance().error(this, "Error during fuzzy checkpoint", e);
    }
  }
}
