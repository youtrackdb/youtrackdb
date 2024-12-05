package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

final class OWALVacuum implements Runnable {

  private final OAbstractPaginatedStorage storage;

  public OWALVacuum(OAbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  @Override
  public void run() {
    storage.runWALVacuum();
  }
}
