package com.jetbrains.youtrack.db.internal.core.storage.cache.local;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Collection;
import java.util.concurrent.Callable;

final class FileFlushTask implements Callable<Void> {

  /**
   *
   */
  private final OWOWCache cache;

  private final IntOpenHashSet fileIdSet;

  FileFlushTask(OWOWCache owowCache, final Collection<Integer> fileIds) {
    cache = owowCache;
    this.fileIdSet = new IntOpenHashSet(fileIds);
  }

  @Override
  public Void call() throws Exception {
    return cache.executeFileFlush(this.fileIdSet);
  }
}
