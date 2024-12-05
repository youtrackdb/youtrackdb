package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.OSharedContextEmbedded;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OProduceExecutionStream;
import java.util.Map;

/**
 * Returns an YTResult containing metadata regarding the database
 */
public class FetchFromDistributedMetadataStep extends AbstractExecutionStep {

  public FetchFromDistributedMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private YTResult produce(OCommandContext ctx) {
    YTDatabaseSessionInternal session = ctx.getDatabase();
    OSharedContextEmbedded value = (OSharedContextEmbedded) session.getSharedContext();

    Map<String, Object> map = value.loadDistributedConfig(session);
    YTResultInternal result = new YTResultInternal(session);

    for (var entry : map.entrySet()) {
      result.setProperty(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
