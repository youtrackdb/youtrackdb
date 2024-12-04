package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTClusterDoesNotExistException extends YTStorageException
    implements YTHighLevelException {

  public YTClusterDoesNotExistException(YTClusterDoesNotExistException exception) {
    super(exception);
  }

  public YTClusterDoesNotExistException(String string) {
    super(string);
  }
}
