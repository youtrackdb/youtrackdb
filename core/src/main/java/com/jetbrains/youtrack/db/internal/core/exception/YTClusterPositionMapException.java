package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.cluster.OClusterPositionMap;

/**
 * @since 10/2/2015
 */
public class YTClusterPositionMapException extends YTDurableComponentException {

  @SuppressWarnings("unused")
  public YTClusterPositionMapException(YTClusterPositionMapException exception) {
    super(exception);
  }

  public YTClusterPositionMapException(String message, OClusterPositionMap component) {
    super(message, component);
  }
}
