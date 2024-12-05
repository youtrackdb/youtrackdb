package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

public class YTInvalidDatabaseNameException extends YTException implements YTHighLevelException {

  public YTInvalidDatabaseNameException(final String message) {
    super(message);
  }

  public YTInvalidDatabaseNameException(final YTInvalidDatabaseNameException exception) {
    super(exception);
  }
}
