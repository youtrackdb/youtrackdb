package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;

public class AcquireTimeoutException extends BaseException {

  public AcquireTimeoutException(String message) {
    super(message);
  }

  public AcquireTimeoutException(BaseException exception) {
    super(exception);
  }
}
