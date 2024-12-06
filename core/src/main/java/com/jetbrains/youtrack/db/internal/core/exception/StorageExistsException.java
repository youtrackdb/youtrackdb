package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class StorageExistsException extends StorageException implements HighLevelException {

  public StorageExistsException(StorageExistsException exception) {
    super(exception);
  }

  public StorageExistsException(String string) {
    super(string);
  }
}
