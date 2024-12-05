package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * Assigns a class to documents coming from upstream
 */
public class SetDocumentClassStep extends AbstractExecutionStep {

  private final String targetClass;

  public SetDocumentClassStep(
      OIdentifier targetClass, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass.getStringValue();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result.isEntity()) {
      var element = result.toEntity();
      ((YTDocument) element).setClassName(targetClass);
      if (!(result instanceof YTResultInternal)) {
        result = new YTUpdatableResult(ctx.getDatabase(), element);
      } else {
        ((YTResultInternal) result).setIdentifiable(element);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ SET CLASS\n" + spaces + "  " + this.targetClass;
  }
}
