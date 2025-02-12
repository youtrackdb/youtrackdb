package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.ExecutionStep;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public class CastToEdgeStep extends AbstractExecutionStep {

  public CastToEdgeStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;
    var upstream = prev.start(ctx);
    return upstream.map(CastToEdgeStep::mapResult);
  }

  private static Result mapResult(Result result, CommandContext ctx) {
    if (result.getEntity().orElse(null) instanceof Edge) {
      return result;
    }
    var db = ctx.getDatabaseSession();
    if (result.isEdge()) {
      if (result instanceof ResultInternal) {
        ((ResultInternal) result).setIdentifiable(result.asEntity().toEdge());
      } else {
        result = new ResultInternal(db, result.asEntity().toEdge());
      }
    } else {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Current entity is not a vertex: " + result);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var result = ExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO EDGE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new CastToEdgeStep(ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
