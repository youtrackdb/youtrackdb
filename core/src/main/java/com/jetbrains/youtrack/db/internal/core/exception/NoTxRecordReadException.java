package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class NoTxRecordReadException extends DatabaseException implements HighLevelException {

  public NoTxRecordReadException(String string) {
    super(string);
  }
}
