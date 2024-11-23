package com.orientechnologies.common.exception;

/**
 * @since 9/28/2015
 */
public class OSystemException extends OException {

  public OSystemException(OSystemException exception) {
    super(exception);
  }

  public OSystemException(String message) {
    super(message);
  }
}
