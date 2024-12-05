package com.jetbrains.youtrack.db.internal.core.util;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import java.util.Optional;

/**
 *
 */
public class OURLConnection {

  private final String url;
  private final String type;
  private final String path;
  private final String dbName;
  private final Optional<ODatabaseType> dbType;

  public OURLConnection(String url, String type, String path, String dbName) {
    this(url, type, path, dbName, Optional.empty());
  }

  public OURLConnection(
      String url, String type, String path, String dbName, Optional<ODatabaseType> dbType) {
    this.url = url;
    this.type = type;
    this.path = path;
    this.dbName = dbName;
    this.dbType = dbType;
  }

  public String getDbName() {
    return dbName;
  }

  public String getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public String getUrl() {
    return url;
  }

  public Optional<ODatabaseType> getDbType() {
    return dbType;
  }

  @Override
  public String toString() {
    return "OURLConnection{"
        + "url='"
        + url
        + "', type='"
        + type
        + "', path='"
        + path
        + "', dbName='"
        + dbName
        + "', dbType="
        + dbType
        + '}';
  }
}
