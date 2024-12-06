package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.cluster.ClusterPositionMap;

/**
 * @since 10/2/2015
 */
public class ClusterPositionMapException extends DurableComponentException {

  @SuppressWarnings("unused")
  public ClusterPositionMapException(ClusterPositionMapException exception) {
    super(exception);
  }

  public ClusterPositionMapException(String message, ClusterPositionMap component) {
    super(message, component);
  }
}
