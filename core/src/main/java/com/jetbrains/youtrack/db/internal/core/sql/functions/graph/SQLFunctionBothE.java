package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public class SQLFunctionBothE extends SQLFunctionMove {

  public static final String NAME = "bothE";

  public SQLFunctionBothE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final DatabaseSessionInternal graph, final Identifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, Direction.BOTH, iLabels);
  }
}
