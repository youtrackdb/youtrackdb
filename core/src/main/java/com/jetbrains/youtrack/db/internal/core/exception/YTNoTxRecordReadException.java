package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

public class YTNoTxRecordReadException extends YTDatabaseException implements YTHighLevelException {

  public YTNoTxRecordReadException(String string) {
    super(string);
  }
}
