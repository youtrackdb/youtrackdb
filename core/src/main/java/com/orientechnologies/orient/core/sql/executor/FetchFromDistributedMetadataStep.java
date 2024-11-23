package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;
import java.util.Map;

/**
 * Returns an OResult containing metadata regarding the database
 */
public class FetchFromDistributedMetadataStep extends AbstractExecutionStep {

  public FetchFromDistributedMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private OResult produce(OCommandContext ctx) {
    ODatabaseSessionInternal session = ctx.getDatabase();
    OSharedContextEmbedded value = (OSharedContextEmbedded) session.getSharedContext();

    Map<String, Object> map = value.loadDistributedConfig(session);
    OResultInternal result = new OResultInternal();

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
