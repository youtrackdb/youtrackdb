package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.api.query.ResultSet;

public class QueryDatabaseState {

  private ResultSet resultSet = null;

  public QueryDatabaseState() {
  }

  public QueryDatabaseState(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public void setResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public void close() {
    if (resultSet != null) {
      resultSet.close();
    }

  }
}
