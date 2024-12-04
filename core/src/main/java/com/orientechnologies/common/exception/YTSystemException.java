package com.orientechnologies.common.exception;

/**
 * @since 9/28/2015
 */
public class YTSystemException extends YTException {

  public YTSystemException(YTSystemException exception) {
    super(exception);
  }

  public YTSystemException(String message) {
    super(message);
  }
}
