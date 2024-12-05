package com.jetbrains.youtrack.db.internal.core.storage.cache.local;

import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import java.util.concurrent.Callable;

final class DeleteFileTask implements Callable<ORawPair<String, String>> {

  /**
   *
   */
  private final OWOWCache cache;

  private final long externalFileId;

  DeleteFileTask(OWOWCache owowCache, long externalFileId) {
    cache = owowCache;
    this.externalFileId = externalFileId;
  }

  @Override
  public ORawPair<String, String> call() throws Exception {
    return cache.executeDeleteFile(externalFileId);
  }
}
