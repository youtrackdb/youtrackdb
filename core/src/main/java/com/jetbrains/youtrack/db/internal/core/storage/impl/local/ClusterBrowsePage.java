package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;

public class ClusterBrowsePage implements Iterable<ClusterBrowseEntry> {

  private final List<ClusterBrowseEntry> entries;
  private final long lastPosition;

  public ClusterBrowsePage(List<ClusterBrowseEntry> entries, long lastPosition) {
    this.entries = entries;
    this.lastPosition = lastPosition;
  }

  @Override
  public Iterator<ClusterBrowseEntry> iterator() {
    return entries.iterator();
  }

  @Override
  public Spliterator<ClusterBrowseEntry> spliterator() {
    return entries.spliterator();
  }

  public long getLastPosition() {
    return lastPosition;
  }
}
