package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.db.document.QueryLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class LocalResultSetLifecycleDecorator implements ResultSet {

  private static final AtomicLong counter = new AtomicLong(0);

  private final ResultSet entity;
  private final List<QueryLifecycleListener> lifecycleListeners = new ArrayList<>();
  private final String queryId;

  private boolean hasNextPage;

  public LocalResultSetLifecycleDecorator(ResultSet entity) {
    this.entity = entity;
    queryId = System.currentTimeMillis() + "_" + counter.incrementAndGet();
  }

  public void addLifecycleListener(QueryLifecycleListener db) {
    this.lifecycleListeners.add(db);
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = entity.hasNext();
    if (!hasNext) {
      close();
    }
    return hasNext;
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }

    return entity.next();
  }

  @Override
  public void close() {
    entity.close();
    this.lifecycleListeners.forEach(x -> x.queryClosed(this.queryId));
    this.lifecycleListeners.clear();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return entity.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return entity.getQueryStats();
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean hasNextPage() {
    return hasNextPage;
  }

  public void setHasNextPage(boolean b) {
    this.hasNextPage = b;
  }

  public boolean isDetached() {
    return entity instanceof InternalResultSet;
  }

  public ResultSet getInternal() {
    return entity;
  }
}
