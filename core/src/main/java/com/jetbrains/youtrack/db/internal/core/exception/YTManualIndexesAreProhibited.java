package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

/**
 * Exception which is thrown to inform user that manual indexes are prohibited.
 */
public class YTManualIndexesAreProhibited extends YTException implements YTHighLevelException {

  public YTManualIndexesAreProhibited(YTManualIndexesAreProhibited exception) {
    super(exception);
  }

  public YTManualIndexesAreProhibited(String message) {
    super(message);
  }
}
