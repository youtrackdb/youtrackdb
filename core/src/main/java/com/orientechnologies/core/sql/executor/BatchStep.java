package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OBatch;

/**
 *
 */
public class BatchStep extends AbstractExecutionStep {

  private final Integer batchSize;

  public BatchStep(OBatch batch, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    batchSize = batch.evaluate(ctx);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream prevResult = prev.start(ctx);
    return prevResult.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ BATCH COMMIT EVERY " + batchSize;
  }
}
