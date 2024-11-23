package com.orientechnologies.common.concur;

import com.orientechnologies.common.exception.OSystemException;

/**
 *
 */
public class OOfflineNodeException extends OSystemException {

  public OOfflineNodeException(OOfflineNodeException exception) {
    super(exception);
  }

  public OOfflineNodeException(String message) {
    super(message);
  }
}
