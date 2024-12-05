package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import java.util.Optional;

/**
 * Deletes records coming from upstream steps
 */
public class DeleteStep extends AbstractExecutionStep {

  public DeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    Optional<YTRID> id = result.getIdentity();
    if (id.isPresent()) {
      ctx.getDatabase().delete(id.get());
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DELETE");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new DeleteStep(ctx, this.profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
