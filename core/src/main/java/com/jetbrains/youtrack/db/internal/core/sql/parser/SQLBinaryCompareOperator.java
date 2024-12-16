package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder;

/**
 *
 */
public interface SQLBinaryCompareOperator {

  boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();

  void toGenericStatement(StringBuilder builder);

  SQLBinaryCompareOperator copy();

  default boolean isRangeOperator() {
    return false;
  }

  IndexFinder.Operation getOperation();
}
