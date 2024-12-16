package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.ExecutionPlan;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public interface InternalExecutionPlan extends ExecutionPlan {

  String JAVA_TYPE = "javaType";

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the
   * execution contains less than N elements, then the result will contain them all, next result(s)
   * will contain zero elements
   *
   * @return
   */
  ExecutionStream start();

  void reset(CommandContext ctx);

  CommandContext getContext();

  long getCost();

  default Result serialize(DatabaseSessionInternal db) {
    throw new UnsupportedOperationException();
  }

  default void deserialize(Result serializedExecutionPlan) {
    throw new UnsupportedOperationException();
  }

  default InternalExecutionPlan copy(CommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  boolean canBeCached();

  default String getStatement() {
    return null;
  }

  default void setStatement(String stm) {
  }

  default String getGenericStatement() {
    return null;
  }

  default void setGenericStatement(String stm) {
  }
}
