package com.jetbrains.youtrack.db.internal.core.storage.cluster;

import com.jetbrains.youtrack.db.internal.core.config.StoragePaginatedClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;
import java.io.IOException;

public abstract class PaginatedCluster extends DurableComponent implements StorageCluster {

  public enum RECORD_STATUS {
    NOT_EXISTENT,
    PRESENT,
    ALLOCATED,
    REMOVED
  }

  public static final String DEF_EXTENSION = ".pcl";

  @SuppressWarnings("SameReturnValue")
  public static int getLatestBinaryVersion() {
    return 2;
  }

  protected PaginatedCluster(
      final AbstractPaginatedStorage storage,
      final String name,
      final String extension,
      final String lockName) {
    super(storage, name, extension, lockName);
  }

  public abstract RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException;

  public abstract StoragePaginatedClusterConfiguration generateClusterConfig();

  public abstract long getFileId();
}
