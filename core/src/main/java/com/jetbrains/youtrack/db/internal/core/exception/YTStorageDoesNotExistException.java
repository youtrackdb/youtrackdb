package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

public class YTStorageDoesNotExistException extends YTStorageException
    implements YTHighLevelException {

  public YTStorageDoesNotExistException(YTStorageDoesNotExistException exception) {
    super(exception);
  }

  public YTStorageDoesNotExistException(String string) {
    super(string);
  }
}
