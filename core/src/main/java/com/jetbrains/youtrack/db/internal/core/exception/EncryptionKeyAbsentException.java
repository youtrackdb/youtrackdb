package com.jetbrains.youtrack.db.internal.core.exception;

public class EncryptionKeyAbsentException extends StorageException {

  public EncryptionKeyAbsentException(EncryptionKeyAbsentException exception) {
    super(exception);
  }

  public EncryptionKeyAbsentException(String string) {
    super(string);
  }
}
