package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.cluster.ClusterPositionMap;

/**
 * @since 10/2/2015
 */
public class YTClusterPositionMapException extends YTDurableComponentException {

  @SuppressWarnings("unused")
  public YTClusterPositionMapException(YTClusterPositionMapException exception) {
    super(exception);
  }

  public YTClusterPositionMapException(String message, ClusterPositionMap component) {
    super(message, component);
  }
}
