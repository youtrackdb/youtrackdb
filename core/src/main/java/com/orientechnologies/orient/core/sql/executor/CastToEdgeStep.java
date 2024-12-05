package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class CastToEdgeStep extends AbstractExecutionStep {

  public CastToEdgeStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result.getEntity().orElse(null) instanceof YTEdge) {
      return result;
    }
    var db = ctx.getDatabase();
    if (result.isEdge()) {
      if (result instanceof YTResultInternal) {
        ((YTResultInternal) result).setIdentifiable(result.toEntity().toEdge());
      } else {
        result = new YTResultInternal(db, result.toEntity().toEdge());
      }
    } else {
      throw new YTCommandExecutionException("Current element is not a vertex: " + result);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO EDGE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new CastToEdgeStep(ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
