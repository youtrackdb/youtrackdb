package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.record.ODirection;

/**
 *
 */
public class OSQLFunctionInV extends OSQLFunctionMove {

  public static final String NAME = "inV";

  public OSQLFunctionInV() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return e2v(graph, iRecord, ODirection.IN, iLabels);
  }
}
