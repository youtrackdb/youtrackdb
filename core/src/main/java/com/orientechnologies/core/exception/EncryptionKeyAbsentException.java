package com.orientechnologies.core.exception;

public class EncryptionKeyAbsentException extends YTStorageException {

  public EncryptionKeyAbsentException(EncryptionKeyAbsentException exception) {
    super(exception);
  }

  public EncryptionKeyAbsentException(String string) {
    super(string);
  }
}
