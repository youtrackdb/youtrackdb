package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import java.io.Serializable;
import java.util.List;


public interface ExecutionPlan extends Serializable {
  List<ExecutionStep> getSteps();
  String prettyPrint(int depth, int indent);

  Result toResult(DatabaseSession db);
}
