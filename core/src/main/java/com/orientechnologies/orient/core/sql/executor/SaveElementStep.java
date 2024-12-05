package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 *
 */
public class SaveElementStep extends AbstractExecutionStep {

  private final OIdentifier cluster;

  public SaveElementStep(OCommandContext ctx, OIdentifier cluster, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.cluster = cluster;
  }

  public SaveElementStep(OCommandContext ctx, boolean profilingEnabled) {
    this(ctx, null, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result.isEntity()) {
      var db = ctx.getDatabase();

      if (cluster == null) {
        db.save(result.getEntity().orElse(null));
      } else {
        db.save(result.getEntity().orElse(null), cluster.getStringValue());
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SAVE RECORD");
    if (cluster != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on cluster ").append(cluster);
    }
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new SaveElementStep(ctx, cluster == null ? null : cluster.copy(), profilingEnabled);
  }
}
