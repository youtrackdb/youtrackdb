package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder;

/**
 *
 */
public interface OBinaryCompareOperator {

  boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();

  void toGenericStatement(StringBuilder builder);

  OBinaryCompareOperator copy();

  default boolean isRangeOperator() {
    return false;
  }

  OIndexFinder.Operation getOperation();
}
