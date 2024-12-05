package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import java.util.Collections;

/**
 *
 */
public class FetchFromVariableStep extends AbstractExecutionStep {

  private String variableName;

  public FetchFromVariableStep(String variableName, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.variableName = variableName;
    reset();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Object src = ctx.getVariable(variableName);
    OExecutionStream source;
    if (src instanceof OExecutionStream) {
      source = (OExecutionStream) src;
    } else if (src instanceof YTResultSet) {
      source =
          OExecutionStream.resultIterator(((YTResultSet) src).stream().iterator())
              .onClose((context) -> ((YTResultSet) src).close());
    } else if (src instanceof YTEntity) {
      source =
          OExecutionStream.resultIterator(
              Collections.singleton(
                  (YTResult) new YTResultInternal(ctx.getDatabase(), (YTEntity) src)).iterator());
    } else if (src instanceof YTResult) {
      source = OExecutionStream.resultIterator(Collections.singleton((YTResult) src).iterator());
    } else if (src instanceof Iterable) {
      source = OExecutionStream.iterator(((Iterable<?>) src).iterator());
    } else {
      throw new YTCommandExecutionException("Cannot use variable as query target: " + variableName);
    }
    return source;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM VARIABLE\n"
        + OExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + variableName;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("variableName", variableName);
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("variableName") != null) {
        this.variableName = fromResult.getProperty(variableName);
      }
      reset();
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
