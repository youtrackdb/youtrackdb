package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OProjectionItem;
import java.util.Collections;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final OProjectionItem item;

  public GuaranteeEmptyCountStep(
      OProjectionItem oProjectionItem, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.item = oProjectionItem;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    OExecutionStream upstream = prev.start(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream.limit(1);
    } else {
      YTResultInternal result = new YTResultInternal(ctx.getDatabase());
      result.setProperty(item.getProjectionAliasAsString(), 0L);
      return OExecutionStream.resultIterator(Collections.singleton((YTResult) result).iterator());
    }
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GuaranteeEmptyCountStep(item.copy(), ctx, profilingEnabled);
  }

  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ GUARANTEE FOR ZERO COUNT ";
  }
}
