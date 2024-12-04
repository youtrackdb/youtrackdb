package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTStorageExistsException extends YTStorageException implements YTHighLevelException {

  public YTStorageExistsException(YTStorageExistsException exception) {
    super(exception);
  }

  public YTStorageExistsException(String string) {
    super(string);
  }
}
