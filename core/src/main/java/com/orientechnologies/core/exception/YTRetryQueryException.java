package com.orientechnologies.core.exception;

import com.orientechnologies.core.index.OIndexAbstract;

/**
 * Exception which is thrown by core components to ask command handler to rebuild and run executed
 * command again.
 *
 * @see OIndexAbstract#getRebuildVersion()
 */
public abstract class YTRetryQueryException extends YTCoreException {

  public YTRetryQueryException(YTRetryQueryException exception) {
    super(exception);
  }

  public YTRetryQueryException(String message) {
    super(message);
  }
}
