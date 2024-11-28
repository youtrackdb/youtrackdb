package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseStats;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class OExplainResultSet implements OResultSet {

  private final OExecutionPlan executionPlan;
  private final ODatabaseStats dbStats;
  boolean hasNext = true;
  private final ODatabaseSessionInternal db;

  public OExplainResultSet(ODatabaseSessionInternal db, OExecutionPlan executionPlan,
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
  public OResult next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }

    OResultInternal result = new OResultInternal(db);
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
