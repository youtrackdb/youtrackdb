package com.jetbrains.youtrack.db.internal.core.metadata.function;

import com.jetbrains.youtrack.db.api.exception.BaseException;

/**
 *
 */
public class FunctionDuplicatedException extends BaseException {

  public FunctionDuplicatedException(String message) {
    super(message);
  }
}
