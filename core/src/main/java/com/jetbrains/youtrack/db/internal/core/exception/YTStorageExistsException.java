package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

public class YTStorageExistsException extends YTStorageException implements YTHighLevelException {

  public YTStorageExistsException(YTStorageExistsException exception) {
    super(exception);
  }

  public YTStorageExistsException(String string) {
    super(string);
  }
}
