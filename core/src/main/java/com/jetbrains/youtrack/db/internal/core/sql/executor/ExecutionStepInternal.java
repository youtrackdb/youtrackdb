package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Execution Steps are the building blocks of a query execution plan
 *
 * <p>Typically an execution plan is made of a chain of steps. The execution is pull-based, meaning
 * that the result set that the client iterates is conceptually the one returned by <i>last</i> step
 * of the execution plan
 *
 * <p>At each `next()` invocation, the step typically fetches a record from the previous (upstream)
 * step, does its elaboration (eg. for a filtering step, it can discard the record and fetch another
 * one if it doesn't match the conditions) and returns the elaborated step
 *
 * <p>
 *
 * <p>The invocation of <code>syncPull(ctx, nResults)</code> has to return a result set of at most
 * nResults records. If the upstream (the previous steps) return more records, they have to be
 * returned by next call of <code>syncPull()</code>. The returned result set can have less than
 * nResults records ONLY if current step cannot produce any more records (eg. the upstream does not
 * have any more records)
 */
public interface ExecutionStepInternal extends ExecutionStep {

  ExecutionStream start(CommandContext ctx) throws YTTimeoutException;

  void sendTimeout();

  void setPrevious(ExecutionStepInternal step);

  void setNext(ExecutionStepInternal step);

  void close();

  static String getIndent(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < indent; j++) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  default String prettyPrint(int depth, int indent) {
    String spaces = getIndent(depth, indent);
    return spaces + getClass().getSimpleName();
  }

  default String getName() {
    return getClass().getSimpleName();
  }

  default String getType() {
    return getClass().getSimpleName();
  }

  default String getDescription() {
    return prettyPrint(0, 3);
  }

  default String getTargetNode() {
    return "<local>";
  }

  default List<ExecutionStep> getSubSteps() {
    return Collections.EMPTY_LIST;
  }

  default List<OExecutionPlan> getSubExecutionPlans() {
    return Collections.EMPTY_LIST;
  }

  default void reset() {
    // do nothing
  }

  default YTResult serialize(YTDatabaseSessionInternal db) {
    throw new UnsupportedOperationException();
  }

  default void deserialize(YTResult fromResult) {
    throw new UnsupportedOperationException();
  }

  static YTResultInternal basicSerialize(YTDatabaseSessionInternal db,
      ExecutionStepInternal step) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty(OInternalExecutionPlan.JAVA_TYPE, step.getClass().getName());
    if (step.getSubSteps() != null && !step.getSubSteps().isEmpty()) {
      List<YTResult> serializedSubsteps = new ArrayList<>();
      for (ExecutionStep substep : step.getSubSteps()) {
        serializedSubsteps.add(((ExecutionStepInternal) substep).serialize(db));
      }
      result.setProperty("subSteps", serializedSubsteps);
    }

    if (step.getSubExecutionPlans() != null && !step.getSubExecutionPlans().isEmpty()) {
      List<YTResult> serializedSubPlans = new ArrayList<>();
      for (OExecutionPlan substep : step.getSubExecutionPlans()) {
        serializedSubPlans.add(((OInternalExecutionPlan) substep).serialize(db));
      }
      result.setProperty("subExecutionPlans", serializedSubPlans);
    }
    return result;
  }

  static void basicDeserialize(YTResult serialized, ExecutionStepInternal step)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    List<YTResult> serializedSubsteps = serialized.getProperty("subSteps");
    if (serializedSubsteps != null) {
      for (YTResult serializedSub : serializedSubsteps) {
        String className = serializedSub.getProperty(OInternalExecutionPlan.JAVA_TYPE);
        ExecutionStepInternal subStep =
            (ExecutionStepInternal) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubSteps().add(subStep);
      }
    }

    List<YTResult> serializedPlans = serialized.getProperty("subExecutionPlans");
    if (serializedSubsteps != null) {
      for (YTResult serializedSub : serializedPlans) {
        String className = serializedSub.getProperty(OInternalExecutionPlan.JAVA_TYPE);
        OInternalExecutionPlan subStep =
            (OInternalExecutionPlan) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubExecutionPlans().add(subStep);
      }
    }
  }

  default ExecutionStep copy(CommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  default boolean canBeCached() {
    return false;
  }
}
