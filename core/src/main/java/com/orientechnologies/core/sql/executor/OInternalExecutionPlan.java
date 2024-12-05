package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public interface OInternalExecutionPlan extends OExecutionPlan {

  String JAVA_TYPE = "javaType";

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the
   * execution contains less than N elements, then the result will contain them all, next result(s)
   * will contain zero elements
   *
   * @return
   */
  OExecutionStream start();

  void reset(OCommandContext ctx);

  OCommandContext getContext();

  long getCost();

  default YTResult serialize(YTDatabaseSessionInternal db) {
    throw new UnsupportedOperationException();
  }

  default void deserialize(YTResult serializedExecutionPlan) {
    throw new UnsupportedOperationException();
  }

  default OInternalExecutionPlan copy(OCommandContext ctx) {
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
