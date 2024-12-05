package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
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
      CommandContext ctx, IntArrayList filterClusterIds, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.filterClusterIds = filterClusterIds;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {

    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    ExecutionStream resultSet = prev.start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private YTResult filterMap(YTResult result, CommandContext ctx) {
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
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
  public ExecutionStep copy(CommandContext ctx) {
    return new GetValueFromIndexEntryStep(ctx, this.filterClusterIds, this.profilingEnabled);
  }
}
