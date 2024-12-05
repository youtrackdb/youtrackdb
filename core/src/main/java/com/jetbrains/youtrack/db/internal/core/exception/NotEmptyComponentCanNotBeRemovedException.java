package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

/**
 * Exception is thrown to inform that non-empty component can not be removed.
 */
public class NotEmptyComponentCanNotBeRemovedException extends YTException
    implements YTHighLevelException {

  public NotEmptyComponentCanNotBeRemovedException(
      NotEmptyComponentCanNotBeRemovedException exception) {
    super(exception);
  }

  public NotEmptyComponentCanNotBeRemovedException(final String message) {
    super(message);
  }
}
