package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.MultipleExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStreamProducer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class FetchFromClassExecutionStep extends AbstractExecutionStep {

  protected String className;
  protected boolean orderByRidAsc = false;
  protected boolean orderByRidDesc = false;
  protected List<ExecutionStep> subSteps = new ArrayList<>();

  protected FetchFromClassExecutionStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  public FetchFromClassExecutionStep(
      String className,
      Set<String> clusters,
      CommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    this(className, clusters, null, ctx, ridOrder, profilingEnabled);
  }

  /**
   * iterates over a class and its subclasses
   *
   * @param className the class name
   * @param clusters  if present (it can be null), filter by only these clusters
   * @param ctx       the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClassExecutionStep(
      String className,
      Set<String> clusters,
      QueryPlanningInfo planningInfo,
      CommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.className = className;

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }
    YTClass clazz = loadClassFromSchema(className, ctx);
    int[] classClusters = clazz.getPolymorphicClusterIds();
    IntArrayList filteredClassClusters = new IntArrayList();

    for (int clusterId : classClusters) {
      String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
      if (clusters == null || clusters.contains(clusterName)) {
        filteredClassClusters.add(clusterId);
      }
    }
    int[] clusterIds = new int[filteredClassClusters.size() + 1];
    for (int i = 0; i < filteredClassClusters.size(); i++) {
      clusterIds[i] = filteredClassClusters.getInt(i);
    }
    clusterIds[clusterIds.length - 1] = -1; // temporary cluster, data in tx

    sortClusers(clusterIds);
    for (int clusterId : clusterIds) {
      if (clusterId > 0) {
        FetchFromClusterExecutionStep step =
            new FetchFromClusterExecutionStep(clusterId, planningInfo, ctx, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        subSteps.add(step);
      } else {
        // current tx
        FetchTemporaryFromTxStep step =
            new FetchTemporaryFromTxStep(ctx, className, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        subSteps.add(step);
      }
    }
  }

  protected YTClass loadClassFromSchema(String className, CommandContext ctx) {
    YTClass clazz = ctx.getDatabase().getMetadata().getImmutableSchemaSnapshot()
        .getClass(className);
    if (clazz == null) {
      throw new YTCommandExecutionException("Class " + className + " not found");
    }
    return clazz;
  }

  private void sortClusers(int[] clusterIds) {
    if (orderByRidAsc) {
      Arrays.sort(clusterIds);
    } else if (orderByRidDesc) {
      Arrays.sort(clusterIds);
      // revert order
      for (int i = 0; i < clusterIds.length / 2; i++) {
        int old = clusterIds[i];
        clusterIds[i] = clusterIds[clusterIds.length - 1 - i];
        clusterIds[clusterIds.length - 1 - i] = old;
      }
    }
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    List<ExecutionStep> stepsIter = subSteps;

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<ExecutionStep> iter = stepsIter.iterator();

          @Override
          public ExecutionStream next(CommandContext ctx) {
            ExecutionStep step = iter.next();
            return ((AbstractExecutionStep) step).start(ctx);
          }

          @Override
          public boolean hasNext(CommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(CommandContext ctx) {
          }
        };

    return new MultipleExecutionStream(res)
        .map(
            (result, context) -> {
              context.setVariable("$current", result);
              return result;
            });
  }

  @Override
  public void sendTimeout() {
    for (ExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).sendTimeout();
    }
    if (prev != null) {
      prev.sendTimeout();
    }
  }

  @Override
  public void close() {
    for (ExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).close();
    }
    if (prev != null) {
      prev.close();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM CLASS ").append(className);
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      ExecutionStepInternal step = (ExecutionStepInternal) subSteps.get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < subSteps.size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = ExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("className", className);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      this.className = fromResult.getProperty("className");
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }

  @Override
  public List<ExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    FetchFromClassExecutionStep result = new FetchFromClassExecutionStep(ctx, profilingEnabled);
    result.className = this.className;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps =
        this.subSteps.stream()
            .map(x -> ((ExecutionStepInternal) x).copy(ctx))
            .collect(Collectors.toList());
    return result;
  }
}
