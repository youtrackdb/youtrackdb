package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;

public class YTAcquireTimeoutException extends YTException {

  public YTAcquireTimeoutException(String message) {
    super(message);
  }

  public YTAcquireTimeoutException(YTException exception) {
    super(exception);
  }
}
