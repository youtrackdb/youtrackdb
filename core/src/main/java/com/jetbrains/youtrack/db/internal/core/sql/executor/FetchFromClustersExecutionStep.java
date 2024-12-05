package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStreamProducer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OMultipleExecutionStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class FetchFromClustersExecutionStep extends AbstractExecutionStep {

  private final List<OExecutionStep> subSteps;
  private boolean orderByRidAsc = false;
  private boolean orderByRidDesc = false;

  /**
   * iterates over a class and its subclasses
   *
   * @param clusterIds the clusters
   * @param ctx        the query context
   * @param ridOrder   true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClustersExecutionStep(
      int[] clusterIds, OCommandContext ctx, Boolean ridOrder, boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }

    subSteps = new ArrayList<>();
    sortClusers(clusterIds);
    for (int clusterId : clusterIds) {
      FetchFromClusterExecutionStep step =
          new FetchFromClusterExecutionStep(clusterId, ctx, profilingEnabled);
      if (orderByRidAsc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (orderByRidDesc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      subSteps.add(step);
    }
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
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    List<OExecutionStep> stepsIter = subSteps;

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<OExecutionStep> iter = stepsIter.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            OExecutionStep step = iter.next();
            return ((AbstractExecutionStep) step).start(ctx);
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {
          }
        };

    return new OMultipleExecutionStream(res);
  }

  @Override
  public void sendTimeout() {
    for (OExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).sendTimeout();
    }

    if (prev != null) {
      prev.sendTimeout();
    }
  }

  @Override
  public void close() {
    for (OExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).close();
    }
    if (prev != null) {
      prev.close();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM CLUSTERS");
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      OExecutionStepInternal step = (OExecutionStepInternal) subSteps.get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < subSteps.size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);

    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);

    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }
}
