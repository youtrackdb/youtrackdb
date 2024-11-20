package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OView;
import java.util.Set;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class FetchFromViewExecutionStep extends FetchFromClassExecutionStep {

  public FetchFromViewExecutionStep(
      String className,
      Set<String> clusters,
      QueryPlanningInfo planningInfo,
      OCommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    super(className, clusters, planningInfo, ctx, ridOrder, profilingEnabled);

    ODatabaseSessionInternal database = (ODatabaseSessionInternal) ctx.getDatabase();
    OView view = loadClassFromSchema(className, ctx);
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

  protected OView loadClassFromSchema(String className, OCommandContext ctx) {
    OView clazz =
        ((ODatabaseSessionInternal) ctx.getDatabase())
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getView(className);
    if (clazz == null) {
      throw new OCommandExecutionException("View " + className + " not found");
    }
    return clazz;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM VIEW ").append(className);
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      OExecutionStepInternal step = (OExecutionStepInternal) getSubSteps().get(i);
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
