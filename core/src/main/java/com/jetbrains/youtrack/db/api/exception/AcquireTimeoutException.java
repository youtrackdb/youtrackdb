package com.jetbrains.youtrack.db.api.exception;

public class AcquireTimeoutException extends BaseException {

  public AcquireTimeoutException(String message) {
    super(message);
  }

  public AcquireTimeoutException(BaseException exception) {
    super(exception);
  }
}
