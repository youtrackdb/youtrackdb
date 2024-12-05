package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OIdentifier;

/**
 *
 */
public class GlobalLetExpressionStep extends AbstractExecutionStep {

  private final OIdentifier varname;
  private final OExpression expression;

  private boolean executed = false;

  public GlobalLetExpressionStep(
      OIdentifier varName, OExpression expression, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    calculate(ctx);
    return OExecutionStream.empty();
  }

  private void calculate(OCommandContext ctx) {
    if (executed) {
      return;
    }
    Object value = expression.execute((YTResult) null, ctx);
    ctx.setVariable(varname.getStringValue(), value);
    executed = true;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (once)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GlobalLetExpressionStep(varname.copy(), expression.copy(), ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
