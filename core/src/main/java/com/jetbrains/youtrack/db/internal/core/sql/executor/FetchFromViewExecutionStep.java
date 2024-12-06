package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import java.util.Set;

/**
 *
 */
public class FetchFromViewExecutionStep extends FetchFromClassExecutionStep {

  public FetchFromViewExecutionStep(
      String className,
      Set<String> clusters,
      QueryPlanningInfo planningInfo,
      CommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    super(className, clusters, planningInfo, ctx, ridOrder, profilingEnabled);

    DatabaseSessionInternal database = ctx.getDatabase();
    SchemaView view = loadClassFromSchema(className, ctx);
    int[] classClusters = view.getPolymorphicClusterIds();
    for (int clusterId : classClusters) {
      String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
      if (clusters == null || clusters.contains(clusterName)) {
        database.queryStartUsingViewCluster(clusterId);
      }
    }
  }

  @Override
  public void close() {
    super.close();
  }

  protected SchemaView loadClassFromSchema(String className, CommandContext ctx) {
    SchemaView clazz = ctx.getDatabase().getMetadata().getImmutableSchemaSnapshot()
        .getView(className);
    if (clazz == null) {
      throw new CommandExecutionException("View " + className + " not found");
    }
    return clazz;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM VIEW ").append(className);
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      ExecutionStepInternal step = (ExecutionStepInternal) getSubSteps().get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
