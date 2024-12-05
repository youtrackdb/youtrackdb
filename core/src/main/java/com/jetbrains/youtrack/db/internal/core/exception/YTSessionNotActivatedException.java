package com.jetbrains.youtrack.db.internal.core.exception;

public class YTSessionNotActivatedException extends YTCoreException {

  public YTSessionNotActivatedException(String dbName) {
    super("Session is not activated on current thread for database '" + dbName + "'", null,
        dbName);
  }
}
