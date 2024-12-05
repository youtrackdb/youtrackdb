package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHookV2;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;

/**
 * Reads an upstream result set and returns a new result set that contains copies of the original
 * YTResult instances
 *
 * <p>This is mainly used from statements that need to copy of the original data before modifying
 * it, eg. UPDATE ... RETURN BEFORE
 */
public class CopyRecordContentBeforeUpdateStep extends AbstractExecutionStep {

  public CopyRecordContentBeforeUpdateStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream lastFetched = prev.start(ctx);
    return lastFetched.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    var db = ctx.getDatabase();
    if (result instanceof YTUpdatableResult) {
      YTResultInternal prevValue = new YTResultInternal(db);
      var rec = result.toEntity();
      prevValue.setProperty("@rid", rec.getIdentity());
      prevValue.setProperty("@version", rec.getVersion());
      if (rec instanceof EntityImpl) {
        prevValue.setProperty(
            "@class", ODocumentInternal.getImmutableSchemaClass(((EntityImpl) rec)).getName());
      }
      if (!result.toEntity().getIdentity().isNew()) {
        for (String propName : result.getPropertyNames()) {
          prevValue.setProperty(
              propName, OLiveQueryHookV2.unboxRidbags(result.getProperty(propName)));
        }
      }
      ((YTUpdatableResult) result).previousValue = prevValue;
    } else {
      throw new YTCommandExecutionException("Cannot fetch previous value: " + result);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY RECORD CONTENT BEFORE UPDATE");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
