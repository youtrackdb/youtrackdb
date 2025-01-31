package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.List;

/**
 *
 */
public class DistributedQueryContext {

  private String queryId;
  private DatabaseSessionInternal db;
  private ResultSet resultSet;

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public DatabaseSession getDb() {
    return db;
  }

  public void setDb(DatabaseSession db) {
    this.db = (DatabaseSessionInternal) db;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public void setResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public List<Result> fetchNextPage() {
    var prev = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      db.activateOnCurrentThread();
      resultSet.close();
      db.close();
    } finally {
      if (prev == null) {
        DatabaseRecordThreadLocal.instance().remove();
      } else {
        DatabaseRecordThreadLocal.instance().set(prev);
      }
    }
    return null;
  }

  public void close() {
    var prev = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      db.activateOnCurrentThread();
      resultSet.close();
      db.close();
      ((SharedContextEmbedded) db.getSharedContext())
          .getActiveDistributedQueries()
          .remove(queryId);
    } finally {
      if (prev == null) {
        DatabaseRecordThreadLocal.instance().remove();
      } else {
        DatabaseRecordThreadLocal.instance().set(prev);
      }
    }
  }
}
