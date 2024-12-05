package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.core.sql.parser.OMatchPathItem;

public class MatchFieldTraverser extends MatchEdgeTraverser {

  public MatchFieldTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  public MatchFieldTraverser(YTResult lastUpstreamRecord, OMatchPathItem item) {
    super(lastUpstreamRecord, item);
  }

  protected OExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      // TODO check possible results!
      qR = ((OFieldMatchPathItem) this.item).getExp().execute(startingPoint, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return OExecutionStream.empty();
    }
    if (qR instanceof YTIdentifiable) {
      return OExecutionStream.singleton(new YTResultInternal(
          iCommandContext.getDatabase(), (YTIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      return OExecutionStream.iterator(((Iterable) qR).iterator());
    }
    return OExecutionStream.empty();
  }
}
