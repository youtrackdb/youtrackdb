package com.jetbrains.youtrack.db.internal.core.storage.cluster;

import com.jetbrains.youtrack.db.internal.core.config.OStoragePaginatedClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.OCluster;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.ODurableComponent;
import java.io.IOException;

public abstract class OPaginatedCluster extends ODurableComponent implements OCluster {

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

  protected OPaginatedCluster(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String extension,
      final String lockName) {
    super(storage, name, extension, lockName);
  }

  public abstract RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException;

  public abstract OStoragePaginatedClusterConfiguration generateClusterConfig();

  public abstract long getFileId();
}
