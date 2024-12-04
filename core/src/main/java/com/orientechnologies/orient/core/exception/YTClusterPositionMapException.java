package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;

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
