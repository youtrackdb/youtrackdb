package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class InvalidDatabaseNameException extends BaseException implements HighLevelException {

  public InvalidDatabaseNameException(final String message) {
    super(message);
  }

  public InvalidDatabaseNameException(final InvalidDatabaseNameException exception) {
    super(exception);
  }
}
