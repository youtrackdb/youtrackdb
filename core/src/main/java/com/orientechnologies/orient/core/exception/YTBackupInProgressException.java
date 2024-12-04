package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.YTHighLevelException;

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
