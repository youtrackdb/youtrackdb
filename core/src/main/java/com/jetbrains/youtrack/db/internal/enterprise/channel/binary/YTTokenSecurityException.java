package com.jetbrains.youtrack.db.internal.enterprise.channel.binary;

import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityException;

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
