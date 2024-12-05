package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Optional;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws
 * YTCommandExecutionException in case it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {

  private final String clazz;

  public CheckRecordTypeStep(CommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clazz = className;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    ExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, CommandContext ctx) {
    if (!result.isEntity()) {
      throw new YTCommandExecutionException("record " + result + " is not an instance of " + clazz);
    }
    Entity doc = result.toEntity();
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
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CHECK RECORD TYPE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (ExecutionStepInternal.getIndent(depth, indent) + "  " + clazz);
    return result;
  }
}
