package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class OIteratorResultSet implements OResultSet {

  protected final Iterator iterator;
  protected final YTDatabaseSessionInternal db;

  public OIteratorResultSet(YTDatabaseSessionInternal db, Iterator iter) {
    this.iterator = iter;
    this.db = db;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public OResult next() {
    Object val = iterator.next();
    if (val instanceof OResult) {
      return (OResult) val;
    }

    OResultInternal result;
    if (val instanceof YTIdentifiable) {
      result = new OResultInternal(db, (YTIdentifiable) val);
    } else {
      result = new OResultInternal(db);
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close() {
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
