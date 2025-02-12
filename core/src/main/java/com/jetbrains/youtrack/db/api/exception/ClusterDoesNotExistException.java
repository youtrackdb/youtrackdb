package com.jetbrains.youtrack.db.api.exception;

import com.jetbrains.youtrack.db.internal.core.exception.StorageException;

public class ClusterDoesNotExistException extends StorageException
    implements HighLevelException {

  public ClusterDoesNotExistException(ClusterDoesNotExistException exception) {
    super(exception);
  }

  public ClusterDoesNotExistException(String dbName, String string) {
    super(dbName, string);
  }
}
