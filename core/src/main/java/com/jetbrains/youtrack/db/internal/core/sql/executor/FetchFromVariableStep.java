package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Collections;

/**
 *
 */
public class FetchFromVariableStep extends AbstractExecutionStep {

  private String variableName;

  public FetchFromVariableStep(String variableName, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.variableName = variableName;
    reset();
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Object src = ctx.getVariable(variableName);
    ExecutionStream source;
    if (src instanceof ExecutionStream) {
      source = (ExecutionStream) src;
    } else if (src instanceof YTResultSet) {
      source =
          ExecutionStream.resultIterator(((YTResultSet) src).stream().iterator())
              .onClose((context) -> ((YTResultSet) src).close());
    } else if (src instanceof Entity) {
      source =
          ExecutionStream.resultIterator(
              Collections.singleton(
                  (YTResult) new YTResultInternal(ctx.getDatabase(), (Entity) src)).iterator());
    } else if (src instanceof YTResult) {
      source = ExecutionStream.resultIterator(Collections.singleton((YTResult) src).iterator());
    } else if (src instanceof Iterable) {
      source = ExecutionStream.iterator(((Iterable<?>) src).iterator());
    } else {
      throw new YTCommandExecutionException("Cannot use variable as query target: " + variableName);
    }
    return source;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM VARIABLE\n"
        + ExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + variableName;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = ExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("variableName", variableName);
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("variableName") != null) {
        this.variableName = fromResult.getProperty(variableName);
      }
      reset();
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
