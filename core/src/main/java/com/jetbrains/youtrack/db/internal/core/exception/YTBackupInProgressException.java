package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

/**
 * @since 10/5/2015
 */
public class YTBackupInProgressException extends YTCoreException implements YTHighLevelException {

  public YTBackupInProgressException(YTBackupInProgressException exception) {
    super(exception);
  }

  public YTBackupInProgressException(String message, String componentName, OErrorCode errorCode) {
    super(message, componentName, errorCode);
  }
}
