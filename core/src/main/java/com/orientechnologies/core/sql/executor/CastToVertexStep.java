package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.record.YTVertex;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class CastToVertexStep extends AbstractExecutionStep {

  public CastToVertexStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result.getEntity().orElse(null) instanceof YTVertex) {
      return result;
    }
    var db = ctx.getDatabase();
    if (result.isVertex()) {
      if (result instanceof YTResultInternal) {
        ((YTResultInternal) result).setIdentifiable(result.toEntity().toVertex());
      } else {
        result = new YTResultInternal(db, result.toEntity().toVertex());
      }
    } else {
      throw new YTCommandExecutionException("Current element is not a vertex: " + result);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO VERTEX";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
