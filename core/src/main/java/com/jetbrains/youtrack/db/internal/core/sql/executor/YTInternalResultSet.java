package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.util.OResettable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class YTInternalResultSet implements YTResultSet, OResettable {

  private List<YTResult> content = new ArrayList<>();
  private int next = 0;
  protected OExecutionPlan plan;

  @Override
  public boolean hasNext() {
    return content.size() > next;
  }

  @Override
  public YTResult next() {
    return content.get(next++);
  }

  @Override
  public void close() {
    this.content.clear();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.ofNullable(plan);
  }

  public void setPlan(OExecutionPlan plan) {
    this.plan = plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }

  public void add(YTResult nextResult) {
    content.add(nextResult);
  }

  public void reset() {
    this.next = 0;
  }

  public int size() {
    return content.size();
  }

  public YTInternalResultSet copy() {
    YTInternalResultSet result = new YTInternalResultSet();
    result.content = this.content;
    return result;
  }
}
