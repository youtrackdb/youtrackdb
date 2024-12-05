package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class FetchFromRidsStep extends AbstractExecutionStep {

  private Collection<YTRecordId> rids;

  public FetchFromRidsStep(
      Collection<YTRecordId> rids, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.rids = rids;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }
    return OExecutionStream.loadIterator(this.rids.iterator());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM RIDs\n"
        + OExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + rids;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    if (rids != null) {
      result.setProperty(
          "rids", rids.stream().map(YTRecordId::toString).collect(Collectors.toList()));
    }
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("rids") != null) {
        List<String> ser = fromResult.getProperty("rids");
        rids = ser.stream().map(YTRecordId::new).collect(Collectors.toList());
      }
      reset();
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
