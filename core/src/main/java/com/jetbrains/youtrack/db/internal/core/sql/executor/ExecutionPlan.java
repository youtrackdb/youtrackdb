package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public interface ExecutionPlan extends Serializable {

  List<ExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  Result toResult(DatabaseSessionInternal db);
}
