package com.orientechnologies.common.concur;

import com.orientechnologies.common.exception.YTSystemException;

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
