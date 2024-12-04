package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
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
    } else if (src instanceof OResultSet) {
      source =
          OExecutionStream.resultIterator(((OResultSet) src).stream().iterator())
              .onClose((context) -> ((OResultSet) src).close());
    } else if (src instanceof YTEntity) {
      source =
          OExecutionStream.resultIterator(
              Collections.singleton(
                  (OResult) new OResultInternal(ctx.getDatabase(), (YTEntity) src)).iterator());
    } else if (src instanceof OResult) {
      source = OExecutionStream.resultIterator(Collections.singleton((OResult) src).iterator());
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
  public OResult serialize(YTDatabaseSessionInternal db) {
    OResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("variableName", variableName);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
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
