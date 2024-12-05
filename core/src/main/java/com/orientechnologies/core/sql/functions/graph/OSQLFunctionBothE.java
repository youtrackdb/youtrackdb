package com.orientechnologies.core.sql.functions.graph;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.ODirection;

/**
 *
 */
public class OSQLFunctionBothE extends OSQLFunctionMove {

  public static final String NAME = "bothE";

  public OSQLFunctionBothE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, ODirection.BOTH, iLabels);
  }
}
