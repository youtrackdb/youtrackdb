package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OUpdateRemoveItem;
import java.util.List;

/**
 *
 */
public class UpdateRemoveStep extends AbstractExecutionStep {

  private final List<OUpdateRemoveItem> items;

  public UpdateRemoveStep(
      List<OUpdateRemoveItem> items, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.items = items;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result instanceof YTResultInternal) {
      for (OUpdateRemoveItem item : items) {
        item.applyUpdate((YTResultInternal) result, ctx);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE REMOVE");
    for (OUpdateRemoveItem item : items) {
      result.append("\n");
      result.append(spaces);
      result.append("  ");
      result.append(item.toString());
    }
    return result.toString();
  }
}
