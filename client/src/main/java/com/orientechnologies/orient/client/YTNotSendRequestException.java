package com.orientechnologies.orient.client;

import com.orientechnologies.common.exception.YTSystemException;

public class YTNotSendRequestException extends YTSystemException {

  public YTNotSendRequestException(YTSystemException exception) {
    super(exception);
  }

  public YTNotSendRequestException(String message) {
    super(message);
  }
}
