package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;

/**
 * This step is used just as a gate check to verify that a cluster belongs to a class.
 *
 * <p>It accepts two values: a target cluster (name or SQLCluster) and a class. If the cluster
 * belongs to the class, then the syncPool() returns an empty result set, otherwise it throws an
 * YTCommandExecutionException
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  private SQLCluster cluster;
  private String clusterName;
  private final String targetClass;

  public CheckClusterTypeStep(
      String targetClusterName, String clazz, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterName = targetClusterName;
    this.targetClass = clazz;
  }

  public CheckClusterTypeStep(
      SQLCluster targetCluster, String clazz, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.cluster = targetCluster;
    this.targetClass = clazz;
  }

  @Override
  public ExecutionStream internalStart(CommandContext context) throws YTTimeoutException {
    var prev = this.prev;
    if (prev != null) {
      prev.start(context).close(ctx);
    }

    YTDatabaseSessionInternal db = context.getDatabase();

    int clusterId;
    if (clusterName != null) {
      clusterId = db.getClusterIdByName(clusterName);
    } else if (cluster.getClusterName() != null) {
      clusterId = db.getClusterIdByName(cluster.getClusterName());
    } else {
      clusterId = cluster.getClusterNumber();
      if (db.getClusterNameById(clusterId) == null) {
        throw new YTCommandExecutionException("Cluster not found: " + clusterId);
      }
    }
    if (clusterId < 0) {
      throw new YTCommandExecutionException("Cluster not found: " + clusterName);
    }

    YTClass clazz = db.getMetadata().getImmutableSchemaSnapshot().getClass(targetClass);
    if (clazz == null) {
      throw new YTCommandExecutionException("Class not found: " + targetClass);
    }

    boolean found = false;
    for (int clust : clazz.getPolymorphicClusterIds()) {
      if (clust == clusterId) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new YTCommandExecutionException(
          "Cluster " + clusterId + " does not belong to class " + targetClass);
    }
    return ExecutionStream.empty();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR CLASS");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    result.append("\n");
    result.append(spaces);
    result.append("  ").append(this.targetClass);
    return result.toString();
  }
}
