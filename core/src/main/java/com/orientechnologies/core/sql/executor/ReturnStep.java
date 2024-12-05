package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OSimpleExecStatement;

/**
 *
 */
public class ReturnStep extends AbstractExecutionStep {

  private final OSimpleExecStatement statement;

  public ReturnStep(OSimpleExecStatement statement, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.statement = statement;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    return statement.executeSimple(ctx);
  }
}
