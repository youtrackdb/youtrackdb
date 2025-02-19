package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 * This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the
 * resulting graph is still consistent
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  public RemoveEdgePointersStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;
    var upstream = prev.start(ctx);

    return upstream.map(RemoveEdgePointersStep::mapResult);
  }

  private static Result mapResult(Result result, CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var propNames = result.getPropertyNames();
    for (var propName :
        propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_")).toList()) {
      var val = result.getProperty(propName);
      if (val instanceof EntityInternal entity) {
        var schemaClass = entity.getImmutableSchemaClass(session);
        if (schemaClass != null && schemaClass.isSubClassOf(session, "E")) {
          ((ResultInternal) result).removeProperty(propName);
        }
      } else if (val instanceof Iterable<?> iterable) {
        for (var o : iterable) {
          if (o instanceof EntityInternal entity) {
            var schemaClass = entity.getImmutableSchemaClass(session);
            if (schemaClass != null && schemaClass.isSubClassOf(session, "E")) {
              ((ResultInternal) result).removeProperty(propName);
            }
          }
        }
      }
    }

    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
    var result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
