package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBatch;

/**
 *
 */
public class BatchStep extends AbstractExecutionStep {

  private final Integer batchSize;

  public BatchStep(OBatch batch, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    batchSize = batch.evaluate(ctx);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    ExecutionStream prevResult = prev.start(ctx);
    return prevResult.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, CommandContext ctx) {
    var db = ctx.getDatabase();
    if (db.getTransaction().isActive()) {
      if (db.getTransaction().getEntryCount() % batchSize == 0) {
        db.commit();
        db.begin();
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ BATCH COMMIT EVERY " + batchSize;
  }
}
