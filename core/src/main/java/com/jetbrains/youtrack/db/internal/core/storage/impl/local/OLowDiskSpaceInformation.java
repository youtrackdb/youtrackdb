package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

/**
 * href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 *
 * @since 05/02/15
 */
public class OLowDiskSpaceInformation {

  public final long freeSpace;
  public final long requiredSpace;

  public OLowDiskSpaceInformation(long freeSpace, long requiredSpace) {
    this.freeSpace = freeSpace;
    this.requiredSpace = requiredSpace;
  }
}
