package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class ClusterDoesNotExistException extends StorageException
    implements HighLevelException {

  public ClusterDoesNotExistException(ClusterDoesNotExistException exception) {
    super(exception);
  }

  public ClusterDoesNotExistException(String string) {
    super(string);
  }
}
