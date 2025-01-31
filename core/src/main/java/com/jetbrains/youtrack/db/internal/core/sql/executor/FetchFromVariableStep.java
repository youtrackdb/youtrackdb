package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.Entity;
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
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    var src = ctx.getVariable(variableName);
    ExecutionStream source;
    if (src instanceof ExecutionStream) {
      source = (ExecutionStream) src;
    } else if (src instanceof ResultSet) {
      source =
          ExecutionStream.resultIterator(((ResultSet) src).stream().iterator())
              .onClose((context) -> ((ResultSet) src).close());
    } else if (src instanceof Entity) {
      source =
          ExecutionStream.resultIterator(
              Collections.singleton(
                  (Result) new ResultInternal(ctx.getDatabase(), (Entity) src)).iterator());
    } else if (src instanceof Result) {
      source = ExecutionStream.resultIterator(Collections.singleton((Result) src).iterator());
    } else if (src instanceof Iterable) {
      source = ExecutionStream.iterator(((Iterable<?>) src).iterator());
    } else {
      throw new CommandExecutionException("Cannot use variable as query target: " + variableName);
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
  public Result serialize(DatabaseSessionInternal db) {
    var result = ExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("variableName", variableName);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("variableName") != null) {
        this.variableName = fromResult.getProperty(variableName);
      }
      reset();
    } catch (Exception e) {
      throw BaseException.wrapException(new CommandExecutionException(""), e);
    }
  }
}
