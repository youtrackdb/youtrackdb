package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OJson;

/**
 *
 */
public class UpdateMergeStep extends AbstractExecutionStep {

  private final OJson json;

  public UpdateMergeStep(OJson json, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result instanceof YTResultInternal) {
      if (!(result.getElement().orElse(null) instanceof YTDocument)) {
        ((YTResultInternal) result).setIdentifiable(result.toElement().getRecord());
      }
      if (!(result.getElement().orElse(null) instanceof YTDocument)) {
        return result;
      }
      handleMerge((YTDocument) result.getElement().orElse(null), ctx);
    }
    return result;
  }

  private void handleMerge(YTDocument record, OCommandContext ctx) {
    record.merge(json.toDocument(record, ctx), true, false);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UPDATE MERGE\n" + spaces + "  " + json;
  }
}
