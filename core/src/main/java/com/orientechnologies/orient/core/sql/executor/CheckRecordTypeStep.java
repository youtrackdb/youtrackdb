package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Optional;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws
 * YTCommandExecutionException in case it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {

  private final String clazz;

  public CheckRecordTypeStep(OCommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clazz = className;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (!result.isEntity()) {
      throw new YTCommandExecutionException("record " + result + " is not an instance of " + clazz);
    }
    YTEntity doc = result.toEntity();
    if (doc == null) {
      throw new YTCommandExecutionException("record " + result + " is not an instance of " + clazz);
    }
    Optional<YTClass> schema = doc.getSchemaType();

    if (schema.isEmpty() || !schema.get().isSubClassOf(clazz)) {
      throw new YTCommandExecutionException("record " + result + " is not an instance of " + clazz);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CHECK RECORD TYPE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (OExecutionStepInternal.getIndent(depth, indent) + "  " + clazz);
    return result;
  }
}
