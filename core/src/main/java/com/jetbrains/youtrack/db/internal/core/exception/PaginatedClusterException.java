package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.cluster.PaginatedCluster;

/**
 * @since 10/2/2015
 */
public class PaginatedClusterException extends DurableComponentException {

  public PaginatedClusterException(PaginatedClusterException exception) {
    super(exception);
  }

  public PaginatedClusterException(String message, PaginatedCluster component) {
    super(message, component);
  }
}
