package com.jetbrains.youtrack.db.internal.core.metadata.function;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;

/**
 *
 */
public class YTFunctionDuplicatedException extends YTException {

  public YTFunctionDuplicatedException(String message) {
    super(message);
  }
}
