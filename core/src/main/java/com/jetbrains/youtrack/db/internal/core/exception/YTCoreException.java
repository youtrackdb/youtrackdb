package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;

/**
 * @since 9/28/2015
 */
public abstract class YTCoreException extends YTException {

  private final OErrorCode errorCode;

  private String dbName;
  private String componentName;

  public YTCoreException(final YTCoreException exception) {
    this(exception, null);
  }

  public YTCoreException(final YTCoreException exception, OErrorCode errorCode) {
    super(exception);
    this.dbName = exception.dbName;
    this.componentName = exception.componentName;
    this.errorCode = errorCode;
  }

  public YTCoreException(final String message) {
    this(message, null, (OErrorCode) null);
  }


  public YTCoreException(final String message, final String componentName) {
    this(message, componentName, (OErrorCode) null);
  }

  public YTCoreException(final String message, final String componentName, String dbName) {
    this(message);
    this.dbName = dbName;
    this.componentName = componentName;
  }

  public YTCoreException(
      final String message, final String componentName, final OErrorCode errorCode) {
    super(message);

    this.errorCode = errorCode;
    this.componentName = componentName;
    final ODatabaseRecordThreadLocal instance = ODatabaseRecordThreadLocal.instance();

    final YTDatabaseSessionInternal database = instance.getIfDefined();
    if (database != null) {
      dbName = database.getName();
    } else {
      dbName = null;
    }
  }

  public OErrorCode getErrorCode() {
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
