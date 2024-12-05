package com.jetbrains.youtrack.db.internal.core.exception;

/**
 *
 */
public class YTLiveQueryInterruptedException extends YTCoreException {

  public YTLiveQueryInterruptedException(YTCoreException exception) {
    super(exception);
  }

  public YTLiveQueryInterruptedException(String message) {
    super(message);
  }
}
