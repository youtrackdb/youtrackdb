package com.jetbrains.youtrack.db.internal.core.exception;

public class SessionNotActivatedException extends CoreException {

  public SessionNotActivatedException() {
    super("Session is not activated on current thread for database.");
  }
}
