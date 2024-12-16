package com.jetbrains.youtrack.db.internal.core.exception;

public class InternalErrorException extends CoreException {

  public InternalErrorException(InternalErrorException exception) {
    super(exception);
  }

  public InternalErrorException(String string) {
    super(string);
  }
}
