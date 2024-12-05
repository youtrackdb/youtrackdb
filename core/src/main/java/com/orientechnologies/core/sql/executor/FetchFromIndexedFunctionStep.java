package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OBinaryCondition;
import com.orientechnologies.core.sql.parser.OFromClause;
import java.util.Iterator;

/**
 *
 */
public class FetchFromIndexedFunctionStep extends AbstractExecutionStep {

  private OBinaryCondition functionCondition;
  private OFromClause queryTarget;

  public FetchFromIndexedFunctionStep(
      OBinaryCondition functionCondition,
      OFromClause queryTarget,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    var prev = this.prev;
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<YTIdentifiable> fullResult = init(ctx);
    return OExecutionStream.loadIterator(fullResult).interruptable();
  }

  private Iterator<YTIdentifiable> init(OCommandContext ctx) {
    return functionCondition.executeIndexedFunction(queryTarget, ctx).iterator();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        OExecutionStepInternal.getIndent(depth, indent)
            + "+ FETCH FROM INDEXED FUNCTION "
            + functionCondition.toString();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("functionCondition", this.functionCondition.serialize(db));
    result.setProperty("queryTarget", this.queryTarget.serialize(db));

    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      functionCondition = new OBinaryCondition(-1);
      functionCondition.deserialize(fromResult.getProperty("functionCondition "));

      queryTarget = new OFromClause(-1);
      queryTarget.deserialize(fromResult.getProperty("functionCondition "));

    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
