package com.jetbrains.youtrack.db.internal.core.storage.cluster;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;

public abstract class ClusterPositionMap extends DurableComponent {

  public static final String DEF_EXTENSION = ".cpm";

  public ClusterPositionMap(
      AbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }
}
