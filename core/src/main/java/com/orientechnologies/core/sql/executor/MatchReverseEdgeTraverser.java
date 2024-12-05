package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OMatchPathItem;
import com.orientechnologies.core.sql.parser.ORid;
import com.orientechnologies.core.sql.parser.OWhereClause;

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

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  protected String targetClusterName(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected ORid targetRid(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected OExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return OExecutionStream.empty();
    }
    if (qR instanceof YTResultInternal) {
      return OExecutionStream.singleton((YTResultInternal) qR);
    }
    if (qR instanceof YTIdentifiable) {
      return OExecutionStream.singleton(
          new YTResultInternal(iCommandContext.getDatabase(), (YTIdentifiable) qR));
    }
    if (qR instanceof Iterable iterable) {
      return OExecutionStream.iterator(iterable.iterator());
    }
    return OExecutionStream.empty();
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
