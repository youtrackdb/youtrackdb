package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;

/**
 * @since 10/2/2015
 */
public class OPaginatedClusterException extends ODurableComponentException {

  public OPaginatedClusterException(OPaginatedClusterException exception) {
    super(exception);
  }

  public OPaginatedClusterException(String message, OPaginatedCluster component) {
    super(message, component);
  }
}
