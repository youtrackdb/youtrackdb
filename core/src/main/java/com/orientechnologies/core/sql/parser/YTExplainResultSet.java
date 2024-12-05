package com.orientechnologies.core.sql.parser;

import com.orientechnologies.core.db.ODatabaseStats;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.OExecutionPlan;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class YTExplainResultSet implements YTResultSet {

  private final OExecutionPlan executionPlan;
  private final ODatabaseStats dbStats;
  boolean hasNext = true;
  private final YTDatabaseSessionInternal db;

  public YTExplainResultSet(YTDatabaseSessionInternal db, OExecutionPlan executionPlan,
      ODatabaseStats dbStats) {
    this.executionPlan = executionPlan;
    this.dbStats = dbStats;
    this.db = db;

    assert db == null || db.assertIfNotActive();
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public YTResult next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }

    YTResultInternal result = new YTResultInternal(db);
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.toResult(db)));
    getExecutionPlan()
        .ifPresent(x -> result.setProperty("executionPlanAsString", x.prettyPrint(0, 3)));
    getExecutionPlan().ifPresent(x -> result.setProperty("dbStats", dbStats.toResult(db)));

    hasNext = false;
    return result;
  }

  @Override
  public void close() {
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
