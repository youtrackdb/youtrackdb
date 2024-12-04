package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.versionmap.OVersionPositionMap;

/**
 * @since 10/2/2015
 */
public class YTVersionPositionMapException extends YTDurableComponentException {

  @SuppressWarnings("unused")
  public YTVersionPositionMapException(YTVersionPositionMapException exception) {
    super(exception);
  }

  public YTVersionPositionMapException(String message, OVersionPositionMap component) {
    super(message, component);
  }
}
