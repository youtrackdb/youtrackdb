package com.jetbrains.youtrack.db.internal.common.concur;

import com.jetbrains.youtrack.db.internal.common.exception.SystemException;

/**
 *
 */
public class OfflineNodeException extends SystemException {

  public OfflineNodeException(OfflineNodeException exception) {
    super(exception);
  }

  public OfflineNodeException(String message) {
    super(message);
  }
}
