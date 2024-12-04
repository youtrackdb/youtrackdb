package com.orientechnologies.orient.core.exception;

/**
 * Exception which is thrown by core components to ask command handler to rebuild and run executed
 * command again.
 *
 * @see com.orientechnologies.orient.core.index.OIndexAbstract#getRebuildVersion()
 */
public abstract class YTRetryQueryException extends YTCoreException {

  public YTRetryQueryException(YTRetryQueryException exception) {
    super(exception);
  }

  public YTRetryQueryException(String message) {
    super(message);
  }
}
