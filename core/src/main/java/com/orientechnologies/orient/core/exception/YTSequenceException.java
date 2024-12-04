package com.orientechnologies.orient.core.exception;

/**
 * @since 2/28/2015
 */
public class YTSequenceException extends YTCoreException {

  private static final long serialVersionUID = -2719447287841577672L;

  public YTSequenceException(YTSequenceException exception) {
    super(exception);
  }

  public YTSequenceException(String message) {
    super(message);
  }
}
