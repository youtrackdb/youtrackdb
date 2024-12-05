package com.jetbrains.youtrack.db.internal.core.storage.cache.local.aoc;

public final class FileSegment {

  private final int segmentIndex;
  private int freeSpace;

  public FileSegment(int segmentIndex) {
    this.segmentIndex = segmentIndex;
  }
}
