package com.orientechnologies.orient.core.exception;

public class YTSessionNotActivatedException extends YTCoreException {

  public YTSessionNotActivatedException(String dbName) {
    super("Session is not activated on current thread for database '" + dbName + "'", null,
        dbName);
  }
}
