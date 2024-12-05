package com.orientechnologies.core.db;

import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.List;

/**
 *
 */
public class DistributedQueryContext {

  private String queryId;
  private YTDatabaseSessionInternal db;
  private YTResultSet resultSet;

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public YTDatabaseSession getDb() {
    return db;
  }

  public void setDb(YTDatabaseSession db) {
    this.db = (YTDatabaseSessionInternal) db;
  }

  public YTResultSet getResultSet() {
    return resultSet;
  }

  public void setResultSet(YTResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public List<YTResult> fetchNextPage() {
    YTDatabaseSessionInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      db.activateOnCurrentThread();
      resultSet.close();
      db.close();
    } finally {
      if (prev == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(prev);
      }
    }
    return null;
  }

  public void close() {
    YTDatabaseSessionInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      db.activateOnCurrentThread();
      resultSet.close();
      db.close();
      ((OSharedContextEmbedded) db.getSharedContext())
          .getActiveDistributedQueries()
          .remove(queryId);
    } finally {
      if (prev == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(prev);
      }
    }
  }
}
