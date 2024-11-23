package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.exception.OSecurityException;

/**
 *
 */
public class OTokenSecurityException extends OSecurityException {

  public OTokenSecurityException(OTokenSecurityException exception) {
    super(exception);
  }

  public OTokenSecurityException(String message) {
    super(message);
  }
}
