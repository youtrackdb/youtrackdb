package com.orientechnologies.core.storage;

import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.conflict.ORecordConflictStrategy;
import java.util.Set;

public interface OStorageInfo {

  // MISC
  OStorageConfiguration getConfiguration();

  boolean isAssigningClusterIds();

  Set<String> getClusterNames();

  int getClusters();

  int getDefaultClusterId();

  String getURL();

  ORecordConflictStrategy getRecordConflictStrategy();

  int getClusterIdByName(String lowerCase);

  String getPhysicalClusterNameById(int iClusterId);

  String getName();
}
