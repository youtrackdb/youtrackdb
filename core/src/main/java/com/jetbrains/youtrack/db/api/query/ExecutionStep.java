package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public interface ExecutionStep {

  String getName();

  String getType();

  String getDescription();

  List<ExecutionStep> getSubSteps();

  /**
   * returns the absolute cost (in nanoseconds) of the execution of this step
   *
   * @return the absolute cost (in nanoseconds) of the execution of this step, -1 if not calculated
   */
  default long getCost() {
    return -1L;
  }

  default Result toResult(DatabaseSession db) {
    ResultInternal result = new ResultInternal((DatabaseSessionInternal) db);
    result.setProperty("name", getName());
    result.setProperty("type", getType());
    result.setProperty(InternalExecutionPlan.JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty(
        "subSteps",
        getSubSteps() == null
            ? null
            : getSubSteps().stream().map(x -> x.toResult(db)).collect(Collectors.toList()));
    result.setProperty("description", getDescription());
    return result;
  }
}
