package com.jetbrains.youtrack.db.internal.core.storage.cluster;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.ODurableComponent;

public abstract class OClusterPositionMap extends ODurableComponent {

  public static final String DEF_EXTENSION = ".cpm";

  public OClusterPositionMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }
}
