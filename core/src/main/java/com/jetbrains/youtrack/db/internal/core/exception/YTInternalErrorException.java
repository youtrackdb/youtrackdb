package com.jetbrains.youtrack.db.internal.core.exception;

public class YTInternalErrorException extends YTCoreException {

  public YTInternalErrorException(YTInternalErrorException exception) {
    super(exception);
  }

  public YTInternalErrorException(String string) {
    super(string);
  }
}
