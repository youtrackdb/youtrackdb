package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;

/**
 *
 */
public class OSQLFunctionBothV extends OSQLFunctionMove {

  public static final String NAME = "bothV";

  public OSQLFunctionBothV() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return e2v(graph, iRecord, ODirection.BOTH, iLabels);
  }
}
