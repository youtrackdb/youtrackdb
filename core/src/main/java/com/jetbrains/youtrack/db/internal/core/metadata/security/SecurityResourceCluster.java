package com.jetbrains.youtrack.db.internal.core.metadata.security;

import java.util.Objects;

public class SecurityResourceCluster extends SecurityResource {

  public static final SecurityResourceCluster ALL_CLUSTERS =
      new SecurityResourceCluster("database.cluster.*", "*");
  public static final SecurityResourceCluster SYSTEM_CLUSTERS =
      new SecurityResourceCluster("database.systemclusters", "");

  private final String clusterName;

  public SecurityResourceCluster(String resourceString, String clusterName) {
    super(resourceString);
    this.clusterName = clusterName;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecurityResourceCluster that = (SecurityResourceCluster) o;
    return Objects.equals(clusterName, that.clusterName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterName);
  }
}
