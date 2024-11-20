package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public abstract class OCoreException extends OException {

  private OErrorCode errorCode;

  private String dbName;
  private String componentName;

  public OCoreException(final OCoreException exception) {
    this(exception, null);
  }

  public OCoreException(final OCoreException exception, OErrorCode errorCode) {
    super(exception);
    this.dbName = exception.dbName;
    this.componentName = exception.componentName;
    this.errorCode = errorCode;
  }

  public OCoreException(final String message) {
    this(message, null, null);
  }

  public OCoreException(final String message, final String componentName) {
    this(message, componentName, null);
  }

  public OCoreException(
      final String message, final String componentName, final OErrorCode errorCode) {
    super(message);

    this.errorCode = errorCode;
    this.componentName = componentName;
    final ODatabaseRecordThreadLocal instance = ODatabaseRecordThreadLocal.instance();

    final ODatabaseSessionInternal database = instance.getIfDefined();
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
