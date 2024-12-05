package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTNoTxRecordReadException extends YTDatabaseException implements YTHighLevelException {

  public YTNoTxRecordReadException(String string) {
    super(string);
  }
}
