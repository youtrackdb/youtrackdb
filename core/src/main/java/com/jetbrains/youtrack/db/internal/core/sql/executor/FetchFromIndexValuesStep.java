package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;

/**
 *
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  public FetchFromIndexValuesStep(
      IndexSearchDescriptor desc, boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(desc, orderAsc, ctx, profilingEnabled);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (isOrderAsc()) {
      return OExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VAUES ASC "
          + desc.getIndex().getName();
    } else {
      return OExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VAUES DESC "
          + desc.getIndex().getName();
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
