package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 * @since 9/28/2015
 */
public abstract class CoreException extends BaseException {

  private final ErrorCode errorCode;

  private String dbName;
  private String componentName;

  public CoreException(final CoreException exception) {
    this(exception, null);
  }

  public CoreException(final CoreException exception, ErrorCode errorCode) {
    super(exception);
    this.dbName = exception.dbName;
    this.componentName = exception.componentName;
    this.errorCode = errorCode;
  }

  public CoreException(final String message) {
    this(message, null, (ErrorCode) null);
  }


  public CoreException(final String message, final String componentName) {
    this(message, componentName, (ErrorCode) null);
  }

  public CoreException(final String message, final String componentName, String dbName) {
    this(message);
    this.dbName = dbName;
    this.componentName = componentName;
  }

  public CoreException(
      final String message, final String componentName, final ErrorCode errorCode) {
    super(message);

    this.errorCode = errorCode;
    this.componentName = componentName;
    final DatabaseRecordThreadLocal instance = DatabaseRecordThreadLocal.instance();

    final DatabaseSessionInternal database = instance.getIfDefined();
    if (database != null) {
      dbName = database.getName();
    } else {
      dbName = null;
    }
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public String getDbName() {
    return dbName;
  }

  public String getComponentName() {
    return componentName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  @Override
  public final String getMessage() {
    final StringBuilder builder = new StringBuilder(super.getMessage());

    if (dbName != null) {
      builder.append("\r\n\t").append("DB name=\"").append(dbName).append("\"");
    }
    if (componentName != null) {
      builder.append("\r\n\t").append("Component Name=\"").append(componentName).append("\"");
    }
    if (errorCode != null) {
      builder.append("\r\n\t").append("Error Code=\"").append(errorCode.getCode()).append("\"");
    }

    return builder.toString();
  }
}
