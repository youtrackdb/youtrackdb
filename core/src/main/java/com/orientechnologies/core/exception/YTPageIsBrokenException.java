package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

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
