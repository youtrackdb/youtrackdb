package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.exception.YTSecurityException;

/**
 *
 */
public class YTTokenSecurityException extends YTSecurityException {

  public YTTokenSecurityException(YTTokenSecurityException exception) {
    super(exception);
  }

  public YTTokenSecurityException(String message) {
    super(message);
  }
}
