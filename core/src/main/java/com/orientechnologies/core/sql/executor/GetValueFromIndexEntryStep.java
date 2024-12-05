package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.stream.Collectors;

/**
 *
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final IntArrayList filterClusterIds;

  /**
   * @param ctx              the execution context
   * @param filterClusterIds only extract values from these clusters. Pass null if no filtering is
   *                         needed
   * @param profilingEnabled enable profiling
   */
  public GetValueFromIndexEntryStep(
      OCommandContext ctx, IntArrayList filterClusterIds, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.filterClusterIds = filterClusterIds;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {

    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private YTResult filterMap(YTResult result, OCommandContext ctx) {
    Object finalVal = result.getProperty("rid");
    if (filterClusterIds != null) {
      if (!(finalVal instanceof YTIdentifiable)) {
        return null;
      }
      YTRID rid = ((YTIdentifiable) finalVal).getIdentity();
      boolean found = false;
      for (int filterClusterId : filterClusterIds) {
        if (rid.getClusterId() < 0 || filterClusterId == rid.getClusterId()) {
          found = true;
          break;
        }
      }
      if (!found) {
        return null;
      }
    }
    if (finalVal instanceof YTIdentifiable) {
      return new YTResultInternal(ctx.getDatabase(), (YTIdentifiable) finalVal);

    } else if (finalVal instanceof YTResult) {
      return (YTResult) finalVal;
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXTRACT VALUE FROM INDEX ENTRY";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (filterClusterIds != null) {
      result += "\n";
      result += spaces;
      result += "  filtering clusters [";
      result +=
          filterClusterIds
              .intStream()
              .boxed()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
      result += "]";
    }
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GetValueFromIndexEntryStep(ctx, this.filterClusterIds, this.profilingEnabled);
  }
}
