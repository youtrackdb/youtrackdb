package com.jetbrains.youtrack.db.internal.client;

import com.jetbrains.youtrack.db.internal.common.exception.SystemException;

public class NotSendRequestException extends SystemException {

  public NotSendRequestException(SystemException exception) {
    super(exception);
  }

  public NotSendRequestException(String message) {
    super(message);
  }
}
