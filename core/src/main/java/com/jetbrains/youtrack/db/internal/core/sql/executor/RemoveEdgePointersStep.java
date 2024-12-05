package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;

/**
 * This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the
 * resulting graph is still consistent
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  public RemoveEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);

    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    var propNames = result.getPropertyNames();
    for (String propName :
        propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_")).toList()) {
      Object val = result.getProperty(propName);
      if (val instanceof Entity) {
        if (((Entity) val).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
          ((YTResultInternal) result).removeProperty(propName);
        }
      } else if (val instanceof Iterable<?> iterable) {
        for (Object o : iterable) {
          if (o instanceof Entity) {
            if (((Entity) o).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
              ((YTResultInternal) result).removeProperty(propName);
              break;
            }
          }
        }
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
