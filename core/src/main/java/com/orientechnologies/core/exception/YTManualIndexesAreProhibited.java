package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.exception.YTHighLevelException;

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
