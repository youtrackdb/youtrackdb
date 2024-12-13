package com.jetbrains.youtrack.db.internal.common.exception;

import com.jetbrains.youtrack.db.api.exception.BaseException;

/**
 * @since 9/28/2015
 */
public class SystemException extends BaseException {

  public SystemException(SystemException exception) {
    super(exception);
  }

  public SystemException(String message) {
    super(message);
  }
}
