package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OMatchPathItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ORid;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OWhereClause;

/**
 *
 */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected String targetClassName(OMatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  protected String targetClusterName(OMatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected ORid targetRid(OMatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected ExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, CommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return ExecutionStream.empty();
    }
    if (qR instanceof YTResultInternal) {
      return ExecutionStream.singleton((YTResultInternal) qR);
    }
    if (qR instanceof YTIdentifiable) {
      return ExecutionStream.singleton(
          new YTResultInternal(iCommandContext.getDatabase(), (YTIdentifiable) qR));
    }
    if (qR instanceof Iterable iterable) {
      return ExecutionStream.iterator(iterable.iterator());
    }
    return ExecutionStream.empty();
  }

  @Override
  protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override
  protected String getEndpointAlias() {
    return endPointAlias;
  }
}
