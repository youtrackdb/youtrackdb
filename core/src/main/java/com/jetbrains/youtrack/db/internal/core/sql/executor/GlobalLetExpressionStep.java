package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;

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
