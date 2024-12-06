package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

/**
 * Exception which is thrown by storage when it detects that some pages of files are broken and it
 * switches to "read only" mode.
 */
public class PageIsBrokenException extends StorageException implements HighLevelException {

  @SuppressWarnings("unused")
  public PageIsBrokenException(StorageException exception) {
    super(exception);
  }

  public PageIsBrokenException(String string) {
    super(string);
  }
}
