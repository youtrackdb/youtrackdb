package com.jetbrains.youtrack.db.internal.enterprise.channel.binary;

import com.jetbrains.youtrack.db.api.exception.SecurityException;

/**
 *
 */
public class TokenSecurityException extends SecurityException {

  public TokenSecurityException(TokenSecurityException exception) {
    super(exception);
  }

  public TokenSecurityException(String message) {
    super(message);
  }
}
