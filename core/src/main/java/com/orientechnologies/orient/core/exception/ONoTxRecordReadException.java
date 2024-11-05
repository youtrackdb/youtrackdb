package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class ONoTxRecordReadException extends ODatabaseException implements OHighLevelException {

  public ONoTxRecordReadException(String string) {
    super(string);
  }
}
