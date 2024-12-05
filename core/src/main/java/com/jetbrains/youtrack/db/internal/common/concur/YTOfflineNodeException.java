package com.jetbrains.youtrack.db.internal.common.concur;

import com.jetbrains.youtrack.db.internal.common.exception.YTSystemException;

/**
 *
 */
public class YTOfflineNodeException extends YTSystemException {

  public YTOfflineNodeException(YTOfflineNodeException exception) {
    super(exception);
  }

  public YTOfflineNodeException(String message) {
    super(message);
  }
}
