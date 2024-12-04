package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;

/**
 * @since 10/2/2015
 */
public class YTPaginatedClusterException extends YTDurableComponentException {

  public YTPaginatedClusterException(YTPaginatedClusterException exception) {
    super(exception);
  }

  public YTPaginatedClusterException(String message, OPaginatedCluster component) {
    super(message, component);
  }
}
