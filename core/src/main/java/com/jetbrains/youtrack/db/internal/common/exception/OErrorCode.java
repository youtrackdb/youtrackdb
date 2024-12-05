package com.jetbrains.youtrack.db.internal.common.exception;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.exception.YTBackupInProgressException;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import java.lang.reflect.InvocationTargetException;

/**
 * Enumeration with the error managed by YouTrackDB. This class has been introduced in v.2.2 and
 * little by little will contain all the YouTrackDB managed errors.
 */
public enum OErrorCode {

  // eg.
  QUERY_PARSE_ERROR(
      OErrorCategory.SQL_PARSING, 1, "query parse error", YTQueryParsingException.class),

  BACKUP_IN_PROGRESS(
      OErrorCategory.STORAGE,
      2,
      "You are trying to start a backup, but it is already in progress",
      YTBackupInProgressException.class),

  MVCC_ERROR(
      OErrorCategory.CONCURRENCY_RETRY,
      3,
      "The version of the update is outdated compared to the persistent value, retry",
      YTConcurrentModificationException.class),

  VALIDATION_ERROR(OErrorCategory.VALIDATION, 4, "Record validation failure", YTException.class),

  GENERIC_ERROR(OErrorCategory.SQL_GENERIC, 5, "Generic Error", YTException.class);

  private static final OErrorCode[] codes = new OErrorCode[6];

  static {
    for (OErrorCode code : OErrorCode.values()) {
      codes[code.code] = code;
    }
  }

  private final OErrorCategory category;
  private final int code;
  private final String description;
  private final Class<? extends YTException> exceptionClass;

  OErrorCode(
      OErrorCategory category,
      int code,
      String description,
      Class<? extends YTException> exceptionClass) {
    this.category = category;
    this.code = code;
    this.description = description;
    this.exceptionClass = exceptionClass;
  }

  public int getCode() {
    return code;
  }

  public void throwException() {
    throwException(this.description, null);
  }

  public void throwException(String message) {
    throwException(message, null);
  }

  public void throwException(Throwable parent) {
    throwException(this.description, parent);
  }

  public void throwException(String message, Throwable parent) {
    YTException exc = newException(message, parent);
    throw exc;
  }

  public YTException newException(String message, Throwable parent) {
    final String fullMessage = String.format("%1$06d_%2$06d - %3$s", category.code, code, message);
    try {
      return YTException.wrapException(
          exceptionClass.getConstructor(String.class).newInstance(fullMessage), parent);
    } catch (InstantiationException
             | IllegalAccessException
             | NoSuchMethodException
             | InvocationTargetException e) {
      LogManager.instance().warn(this, "Cannot instantiate exception " + exceptionClass);
    }
    return null;
  }

  public static OErrorCode getErrorCode(int code) {
    return codes[code];
  }
}
