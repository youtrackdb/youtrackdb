package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTStorageDoesNotExistException extends YTStorageException
    implements YTHighLevelException {

  public YTStorageDoesNotExistException(YTStorageDoesNotExistException exception) {
    super(exception);
  }

  public YTStorageDoesNotExistException(String string) {
    super(string);
  }
}
