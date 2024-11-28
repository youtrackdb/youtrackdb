package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result.isElement()) {
      var element = result.toElement();
      ((ODocument) element).setClassName(targetClass);
      if (!(result instanceof OResultInternal)) {
        result = new OUpdatableResult(ctx.getDatabase(), element);
      } else {
        ((OResultInternal) result).setIdentifiable(element);
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
