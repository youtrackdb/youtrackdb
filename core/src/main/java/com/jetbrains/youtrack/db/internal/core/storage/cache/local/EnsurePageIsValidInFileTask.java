package com.jetbrains.youtrack.db.internal.core.storage.cache.local;

final class EnsurePageIsValidInFileTask implements Runnable {

  private final int internalFileId;
  private final int pageIndex;

  private final OWOWCache writeCache;

  EnsurePageIsValidInFileTask(int internalFileId, int pageIndex, OWOWCache writeCache) {
    this.internalFileId = internalFileId;
    this.pageIndex = pageIndex;
    this.writeCache = writeCache;
  }

  @Override
  public void run() {
    writeCache.writeValidPageInFile(internalFileId, pageIndex);
  }
}
