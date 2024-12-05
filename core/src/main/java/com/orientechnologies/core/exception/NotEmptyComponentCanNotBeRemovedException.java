package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.exception.YTHighLevelException;

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
