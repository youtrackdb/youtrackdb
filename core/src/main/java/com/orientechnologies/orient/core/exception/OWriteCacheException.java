package com.orientechnologies.orient.core.exception;

/**
 * @since 9/28/2015
 */
public class OWriteCacheException extends OCoreException {

  public OWriteCacheException(OWriteCacheException exception) {
    super(exception);
  }

  public OWriteCacheException(String message) {
    super(message);
  }
}
