package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Direction;

/**
 *
 */
public class SQLFunctionBothV extends SQLFunctionMove {

  public static final String NAME = "bothV";

  public SQLFunctionBothV() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final DatabaseSession graph, final Identifiable iRecord, final String[] iLabels) {
    return e2v(graph, iRecord, Direction.BOTH, iLabels);
  }
}
