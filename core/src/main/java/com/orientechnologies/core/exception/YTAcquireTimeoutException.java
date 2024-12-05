package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTException;

public class YTAcquireTimeoutException extends YTException {

  public YTAcquireTimeoutException(String message) {
    super(message);
  }

  public YTAcquireTimeoutException(YTException exception) {
    super(exception);
  }
}
