package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OIdentifier;
import com.orientechnologies.core.sql.parser.OProjectionItem;

/**
 *
 */
public class LetExpressionStep extends AbstractExecutionStep {

  private OIdentifier varname;
  private OExpression expression;

  public LetExpressionStep(
      OIdentifier varName, OExpression expression, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException(
          "Cannot execute a local LET on a query without a target");
    }

    return prev.start(ctx).map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    Object value = expression.execute(result, ctx);
    ((YTResultInternal) result)
        .setMetadata(varname.getStringValue(), OProjectionItem.convert(value, ctx));
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    if (varname != null) {
      result.setProperty("varname", varname.serialize(db));
    }
    if (expression != null) {
      result.setProperty("expression", expression.serialize(db));
    }
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("varname") != null) {
        varname = OIdentifier.deserialize(fromResult.getProperty("varname"));
      }
      if (fromResult.getProperty("expression") != null) {
        expression = new OExpression(-1);
        expression.deserialize(fromResult.getProperty("expression"));
      }
      reset();
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
