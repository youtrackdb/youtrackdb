package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFieldMatchPathItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLMatchPathItem;

public class MatchFieldTraverser extends MatchEdgeTraverser {

  public MatchFieldTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  public MatchFieldTraverser(YTResult lastUpstreamRecord, SQLMatchPathItem item) {
    super(lastUpstreamRecord, item);
  }

  protected ExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, CommandContext iCommandContext) {

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      // TODO check possible results!
      qR = ((SQLFieldMatchPathItem) this.item).getExp().execute(startingPoint, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return ExecutionStream.empty();
    }
    if (qR instanceof YTIdentifiable) {
      return ExecutionStream.singleton(new YTResultInternal(
          iCommandContext.getDatabase(), (YTIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      return ExecutionStream.iterator(((Iterable) qR).iterator());
    }
    return ExecutionStream.empty();
  }
}
