package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Set;

/**
 *
 */
public class FilterByClustersStep extends AbstractExecutionStep {

  private Set<String> clusters;

  public FilterByClustersStep(
      Set<String> filterClusters, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusters = filterClusters;
  }

  private IntOpenHashSet init(DatabaseSessionInternal db) {
    var clusterIds = new IntOpenHashSet();
    for (var clusterName : clusters) {
      var clusterId = db.getClusterIdByName(clusterName);
      clusterIds.add(clusterId);
    }
    return clusterIds;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    var ids = init(ctx.getDatabase());
    if (prev == null) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    var resultSet = prev.start(ctx);
    return resultSet.filter((value, context) -> this.filterMap(value, ids));
  }

  private Result filterMap(Result result, IntOpenHashSet clusterIds) {
    if (result.isEntity()) {
      var rid = result.getRecordId();
      assert rid != null;

      var clusterId = rid.getClusterId();
      if (clusterId < 0) {
        // this record comes from a TX, it still doesn't have a cluster assigned
        return result;
      }
      if (clusterIds.contains(clusterId)) {
        return result;
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ FILTER ITEMS BY CLUSTERS \n"
        + ExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + String.join(", ", clusters);
  }

  @Override
  public Result serialize(DatabaseSessionInternal db) {
    var result = ExecutionStepInternal.basicSerialize(db, this);
    if (clusters != null) {
      result.setProperty("clusters", clusters);
    }

    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      clusters = fromResult.getProperty("clusters");
    } catch (Exception e) {
      throw BaseException.wrapException(new CommandExecutionException(""), e);
    }
  }
}
