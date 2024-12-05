package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

public class YTClusterDoesNotExistException extends YTStorageException
    implements YTHighLevelException {

  public YTClusterDoesNotExistException(YTClusterDoesNotExistException exception) {
    super(exception);
  }

  public YTClusterDoesNotExistException(String string) {
    super(string);
  }
}
