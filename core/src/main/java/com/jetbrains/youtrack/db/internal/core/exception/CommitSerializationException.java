package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class CommitSerializationException extends CoreException implements
    HighLevelException {

  private static final long serialVersionUID = -1157631679527219263L;

  public CommitSerializationException(CommitSerializationException exception) {
    super(exception);
  }

  public CommitSerializationException(String message) {
    super(message);
  }
}
