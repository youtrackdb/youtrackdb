package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBinaryCondition;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
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
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    var prev = this.prev;
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<YTIdentifiable> fullResult = init(ctx);
    return ExecutionStream.loadIterator(fullResult).interruptable();
  }

  private Iterator<YTIdentifiable> init(CommandContext ctx) {
    return functionCondition.executeIndexedFunction(queryTarget, ctx).iterator();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent)
            + "+ FETCH FROM INDEXED FUNCTION "
            + functionCondition.toString();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = ExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("functionCondition", this.functionCondition.serialize(db));
    result.setProperty("queryTarget", this.queryTarget.serialize(db));

    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      functionCondition = new OBinaryCondition(-1);
      functionCondition.deserialize(fromResult.getProperty("functionCondition "));

      queryTarget = new OFromClause(-1);
      queryTarget.deserialize(fromResult.getProperty("functionCondition "));

    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
