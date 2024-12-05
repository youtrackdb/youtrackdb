package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

final class OWALVacuum implements Runnable {

  private final AbstractPaginatedStorage storage;

  public OWALVacuum(AbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  @Override
  public void run() {
    storage.runWALVacuum();
  }
}
