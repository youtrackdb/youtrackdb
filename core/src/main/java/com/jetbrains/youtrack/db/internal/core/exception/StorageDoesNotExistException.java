package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class StorageDoesNotExistException extends StorageException
    implements HighLevelException {

  public StorageDoesNotExistException(StorageDoesNotExistException exception) {
    super(exception);
  }

  public StorageDoesNotExistException(String string) {
    super(string);
  }
}
