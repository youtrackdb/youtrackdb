package com.orientechnologies.core.sql.functions.graph;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.ODirection;

/**
 *
 */
public class OSQLFunctionOutE extends OSQLFunctionMove {

  public static final String NAME = "outE";

  public OSQLFunctionOutE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, ODirection.OUT, iLabels);
  }
}
