package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * Reads an upstream result set and returns a new result set that contains copies of the original
 * OResult instances
 *
 * <p>This is mainly used from statements that need to copy of the original data to save it
 * somewhere else, eg. INSERT ... FROM SELECT
 */
public class CopyDocumentStep extends AbstractExecutionStep {

  public CopyDocumentStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(CopyDocumentStep::mapResult);
  }

  private static OResult mapResult(OResult result, OCommandContext ctx) {
    ODocument resultDoc;
    if (result.isElement()) {
      var docToCopy = (ODocument) result.toElement();
      resultDoc = docToCopy.copy();
      resultDoc.getIdentity().reset();
      resultDoc.setClassName(null);
      resultDoc.setDirty();
    } else {
      resultDoc = (ODocument) result.toElement();
    }
    return new OUpdatableResult(ctx.getDatabase(), resultDoc);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY DOCUMENT");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
