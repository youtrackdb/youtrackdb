package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;

/**
 *
 */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private final int size;

  public EmptyDataGeneratorStep(int size, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.size = size;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::create).limit(size);
  }

  private OResult create(OCommandContext ctx) {
    OResultInternal result = new OResultInternal();
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
