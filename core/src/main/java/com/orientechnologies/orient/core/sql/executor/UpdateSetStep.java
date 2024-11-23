package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OUpdateItem;
import java.util.List;

/**
 *
 */
public class UpdateSetStep extends AbstractExecutionStep {

  private final List<OUpdateItem> items;

  public UpdateSetStep(
      List<OUpdateItem> updateItems, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.items = updateItems;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OResultInternal) {
      for (OUpdateItem item : items) {
        item.applyUpdate((OResultInternal) result, ctx);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE SET");

    for (OUpdateItem item : items) {
      result.append("\n");
      result.append(spaces);
      result.append("  ");
      result.append(item.toString());
    }
    return result.toString();
  }
}
