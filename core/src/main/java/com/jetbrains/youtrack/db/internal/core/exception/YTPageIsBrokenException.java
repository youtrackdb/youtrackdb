package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

/**
 * Exception which is thrown by storage when it detects that some pages of files are broken and it
 * switches to "read only" mode.
 */
public class YTPageIsBrokenException extends YTStorageException implements YTHighLevelException {

  @SuppressWarnings("unused")
  public YTPageIsBrokenException(YTStorageException exception) {
    super(exception);
  }

  public YTPageIsBrokenException(String string) {
    super(string);
  }
}
