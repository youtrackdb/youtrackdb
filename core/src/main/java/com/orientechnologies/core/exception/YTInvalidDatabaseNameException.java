package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.exception.YTHighLevelException;

public class YTInvalidDatabaseNameException extends YTException implements YTHighLevelException {

  public YTInvalidDatabaseNameException(final String message) {
    super(message);
  }

  public YTInvalidDatabaseNameException(final YTInvalidDatabaseNameException exception) {
    super(exception);
  }
}
