package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.record.ODirection;

/**
 *
 */
public class OSQLFunctionInE extends OSQLFunctionMove {

  public static final String NAME = "inE";

  public OSQLFunctionInE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, ODirection.IN, iLabels);
  }
}
