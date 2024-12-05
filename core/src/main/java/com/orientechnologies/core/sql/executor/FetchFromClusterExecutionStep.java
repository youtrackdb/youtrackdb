package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.core.sql.parser.OBinaryCondition;
import com.orientechnologies.core.sql.parser.OBooleanExpression;
import com.orientechnologies.core.sql.parser.OGeOperator;
import com.orientechnologies.core.sql.parser.OGtOperator;
import com.orientechnologies.core.sql.parser.OLeOperator;
import com.orientechnologies.core.sql.parser.OLtOperator;
import com.orientechnologies.core.sql.parser.ORid;
import java.util.Iterator;

/**
 *
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC = "ASC";
  public static final Object ORDER_DESC = "DESC";
  private final QueryPlanningInfo queryPlanning;

  private int clusterId;
  private Object order;

  public FetchFromClusterExecutionStep(
      int clusterId, OCommandContext ctx, boolean profilingEnabled) {
    this(clusterId, null, ctx, profilingEnabled);
  }

  public FetchFromClusterExecutionStep(
      int clusterId,
      QueryPlanningInfo queryPlanning,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterId = clusterId;
    this.queryPlanning = queryPlanning;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }
    long minClusterPosition = calculateMinClusterPosition();
    long maxClusterPosition = calculateMaxClusterPosition();
    ORecordIteratorCluster<YTRecord> iterator =
        new ORecordIteratorCluster<>(
            ctx.getDatabase(), clusterId, minClusterPosition, maxClusterPosition);
    Iterator<YTRecord> iter;
    if (ORDER_DESC.equals(order)) {
      iter = iterator.reversed();
    } else {
      iter = iterator;
    }

    OExecutionStream set = OExecutionStream.loadIterator(iter);

    set = set.interruptable();
    return set;
  }

  private long calculateMinClusterPosition() {
    if (queryPlanning == null
        || queryPlanning.ridRangeConditions == null
        || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }

    long maxValue = -1;

    for (OBooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof OBinaryCondition cond) {
        ORid condRid = cond.getRight().getRid();
        OBinaryCompareOperator operator = cond.getOperator();
        if (condRid != null) {
          if (condRid.getCluster().getValue().intValue() != this.clusterId) {
            continue;
          }
          if (operator instanceof OGtOperator || operator instanceof OGeOperator) {
            maxValue = Math.max(maxValue, condRid.getPosition().getValue().longValue());
          }
        }
      }
    }

    return maxValue;
  }

  private long calculateMaxClusterPosition() {
    if (queryPlanning == null
        || queryPlanning.ridRangeConditions == null
        || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }
    long minValue = Long.MAX_VALUE;

    for (OBooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof OBinaryCondition cond) {
        YTRID conditionRid;

        Object obj;
        if (cond.getRight().getRid() != null) {
          obj =
              ((OBinaryCondition) ridRangeCondition)
                  .getRight()
                  .getRid()
                  .toRecordId((YTResult) null, ctx);
        } else {
          obj = ((OBinaryCondition) ridRangeCondition).getRight().execute((YTResult) null, ctx);
        }

        conditionRid = ((YTIdentifiable) obj).getIdentity();
        OBinaryCompareOperator operator = cond.getOperator();
        if (conditionRid != null) {
          if (conditionRid.getClusterId() != this.clusterId) {
            continue;
          }
          if (operator instanceof OLtOperator || operator instanceof OLeOperator) {
            minValue = Math.min(minValue, conditionRid.getClusterPosition());
          }
        }
      }
    }

    return minValue == Long.MAX_VALUE ? -1 : minValue;
  }

  @Override
  public void sendTimeout() {
    super.sendTimeout();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String orderString = ORDER_DESC.equals(order) ? "DESC" : "ASC";
    String result =
        OExecutionStepInternal.getIndent(depth, indent)
            + "+ FETCH FROM CLUSTER "
            + clusterId
            + " "
            + orderString;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("clusterId", clusterId);
    result.setProperty("order", order);
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.clusterId = fromResult.getProperty("clusterId");
      Object orderProp = fromResult.getProperty("order");
      if (orderProp != null) {
        this.order = ORDER_ASC.equals(fromResult.getProperty("order")) ? ORDER_ASC : ORDER_DESC;
      }
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchFromClusterExecutionStep(
        this.clusterId,
        this.queryPlanning == null ? null : this.queryPlanning.copy(),
        ctx,
        profilingEnabled);
  }
}
