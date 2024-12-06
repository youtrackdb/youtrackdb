package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.RecordConflictStrategy;
import java.util.Set;

public interface StorageInfo {

  // MISC
  StorageConfiguration getConfiguration();

  boolean isAssigningClusterIds();

  Set<String> getClusterNames();

  int getClusters();

  int getDefaultClusterId();

  String getURL();

  RecordConflictStrategy getRecordConflictStrategy();

  int getClusterIdByName(String lowerCase);

  String getPhysicalClusterNameById(int iClusterId);

  String getName();
}
