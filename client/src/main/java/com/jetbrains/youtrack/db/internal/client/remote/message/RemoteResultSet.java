package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.api.query.ExecutionPlan;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.internal.client.remote.db.DatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.core.db.QueryDatabaseState;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionAbstract;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class RemoteResultSet implements ResultSet {

  private final DatabaseSessionRemote db;
  private final String queryId;
  private List<Result> currentPage;
  private Optional<ExecutionPlan> executionPlan;
  private Map<String, Long> queryStats;
  private boolean hasNextPage;

  public RemoteResultSet(
      DatabaseSessionRemote db,
      String queryId,
      List<Result> currentPage,
      Optional<ExecutionPlan> executionPlan,
      Map<String, Long> queryStats,
      boolean hasNextPage) {
    this.db = db;
    this.queryId = queryId;
    this.currentPage = currentPage;
    this.executionPlan = executionPlan;
    this.queryStats = queryStats;
    this.hasNextPage = hasNextPage;
    if (db != null) {
      db.queryStarted(queryId, new QueryDatabaseState(this));
      for (Result result : currentPage) {
        if (result instanceof ResultInternal) {
          ((ResultInternal) result).bindToCache(db);
        }
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (!currentPage.isEmpty()) {
      return true;
    }
    if (!hasNextPage()) {
      return false;
    }
    fetchNextPage();
    return !currentPage.isEmpty();
  }

  private void fetchNextPage() {
    if (db != null) {
      db.fetchNextPage(this);
    }
  }

  @Override
  public Result next() {
    if (currentPage.isEmpty()) {
      if (!hasNextPage()) {
        throw new IllegalStateException();
      }
      fetchNextPage();
    }
    if (currentPage.isEmpty()) {
      throw new IllegalStateException();
    }
    Result internal = currentPage.remove(0);

    if (internal.isRecord() && db != null && db.getTransaction().isActive()) {
      DBRecord record = db.getTransaction().getRecord(internal.getIdentity().orElseThrow());
      if (record != null && record != FrontendTransactionAbstract.DELETED_RECORD) {
        internal = new ResultInternal(db, record);
      }
    }
    return internal;
  }

  @Override
  public void close() {
    if (hasNextPage && db != null) {
      // CLOSES THE QUERY SERVER SIDE ONLY IF THERE IS ANOTHER PAGE. THE SERVER ALREADY
      // AUTOMATICALLY CLOSES THE QUERY AFTER SENDING THE LAST PAGE
      db.closeQuery(queryId);
    }
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return queryStats;
  }

  public void add(ResultInternal item) {
    currentPage.add(item);
  }

  public boolean hasNextPage() {
    return hasNextPage;
  }

  public String getQueryId() {
    return queryId;
  }

  public void fetched(
      List<Result> result,
      boolean hasNextPage,
      Optional<ExecutionPlan> executionPlan,
      Map<String, Long> queryStats) {
    this.currentPage = result;
    this.hasNextPage = hasNextPage;

    if (queryStats != null) {
      this.queryStats = queryStats;
    }
    executionPlan.ifPresent(x -> this.executionPlan = executionPlan);
  }
}
