package com.orientechnologies.orient.core.exception;

public class OSessionNotActivatedException extends OCoreException {

  public OSessionNotActivatedException(String dbName) {
    super("Session is not activated on current thread for database '" + dbName + "'", null,
        dbName);
  }
}
