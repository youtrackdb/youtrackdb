package com.jetbrains.youtrack.db.api.exception;

public class NoTxRecordReadException extends DatabaseException implements HighLevelException {

  public NoTxRecordReadException(String string) {
    super(string);
  }
}
