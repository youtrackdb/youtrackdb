package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTCommitSerializationException extends YTCoreException implements
    YTHighLevelException {

  private static final long serialVersionUID = -1157631679527219263L;

  public YTCommitSerializationException(YTCommitSerializationException exception) {
    super(exception);
  }

  public YTCommitSerializationException(String message) {
    super(message);
  }
}
