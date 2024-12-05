package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.db.document.OQueryLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTInternalResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class YTLocalResultSetLifecycleDecorator implements YTResultSet {

  private static final AtomicLong counter = new AtomicLong(0);

  private final YTResultSet entity;
  private final List<OQueryLifecycleListener> lifecycleListeners = new ArrayList<>();
  private final String queryId;

  private boolean hasNextPage;

  public YTLocalResultSetLifecycleDecorator(YTResultSet entity) {
    this.entity = entity;
    queryId = System.currentTimeMillis() + "_" + counter.incrementAndGet();
  }

  public void addLifecycleListener(OQueryLifecycleListener db) {
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
  public YTResult next() {
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
  public Optional<OExecutionPlan> getExecutionPlan() {
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
    return entity instanceof YTInternalResultSet;
  }

  public YTResultSet getInternal() {
    return entity;
  }
}
