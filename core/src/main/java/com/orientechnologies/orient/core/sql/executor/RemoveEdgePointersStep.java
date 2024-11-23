package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the
 * resulting graph is still consistent
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  public RemoveEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);

    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    var propNames = result.getPropertyNames();
    for (String propName :
        propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_")).toList()) {
      Object val = result.getProperty(propName);
      if (val instanceof OElement) {
        if (((OElement) val).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
          ((OResultInternal) result).removeProperty(propName);
        }
      } else if (val instanceof Iterable<?> iterable) {
        for (Object o : iterable) {
          if (o instanceof OElement) {
            if (((OElement) o).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
              ((OResultInternal) result).removeProperty(propName);
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
