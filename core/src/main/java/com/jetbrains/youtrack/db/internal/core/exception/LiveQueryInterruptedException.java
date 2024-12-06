package com.jetbrains.youtrack.db.internal.core.exception;

/**
 *
 */
public class LiveQueryInterruptedException extends CoreException {

  public LiveQueryInterruptedException(CoreException exception) {
    super(exception);
  }

  public LiveQueryInterruptedException(String message) {
    super(message);
  }
}
