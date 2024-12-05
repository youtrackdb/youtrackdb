package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.core.storage.ORawBuffer;

public class OClusterBrowseEntry {

  private final long clusterPosition;
  private final ORawBuffer buffer;

  public OClusterBrowseEntry(long clusterPosition, ORawBuffer buffer) {
    this.clusterPosition = clusterPosition;
    this.buffer = buffer;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public ORawBuffer getBuffer() {
    return buffer;
  }
}
