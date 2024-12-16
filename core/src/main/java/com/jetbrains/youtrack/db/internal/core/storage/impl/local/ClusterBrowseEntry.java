package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.core.storage.RawBuffer;

public class ClusterBrowseEntry {

  private final long clusterPosition;
  private final RawBuffer buffer;

  public ClusterBrowseEntry(long clusterPosition, RawBuffer buffer) {
    this.clusterPosition = clusterPosition;
    this.buffer = buffer;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public RawBuffer getBuffer() {
    return buffer;
  }
}
