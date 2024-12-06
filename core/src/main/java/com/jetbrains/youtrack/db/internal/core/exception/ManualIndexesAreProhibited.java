package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

/**
 * Exception which is thrown to inform user that manual indexes are prohibited.
 */
public class ManualIndexesAreProhibited extends BaseException implements HighLevelException {

  public ManualIndexesAreProhibited(ManualIndexesAreProhibited exception) {
    super(exception);
  }

  public ManualIndexesAreProhibited(String message) {
    super(message);
  }
}
