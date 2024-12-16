package com.jetbrains.youtrack.db.internal.core.exception;

public class InvalidInstanceIdException extends StorageException {

  private static final long serialVersionUID = -287310485157164592L;

  public InvalidInstanceIdException(String string) {
    super(string);
  }
}
