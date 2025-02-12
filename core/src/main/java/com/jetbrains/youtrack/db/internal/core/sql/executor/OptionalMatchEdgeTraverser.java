package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 *
 */
public class OptionalMatchEdgeTraverser extends MatchEdgeTraverser {

  public static final Result EMPTY_OPTIONAL = new ResultInternal(null);

  public OptionalMatchEdgeTraverser(Result lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected void init(CommandContext ctx) {
    if (downstream == null) {
      super.init(ctx);
      if (!downstream.hasNext(ctx)) {
        downstream = ExecutionStream.singleton(EMPTY_OPTIONAL);
      }
    }
  }

  public Result next(CommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext(ctx)) {
      throw new IllegalStateException();
    }

    var endPointAlias = getEndpointAlias();
    var prevValue = sourceRecord.getProperty(endPointAlias);
    var next = downstream.next(ctx);

    if (isEmptyOptional(prevValue)) {
      return sourceRecord;
    }
    if (!isEmptyOptional(next)) {
      if (prevValue != null && !equals(prevValue, next.getEntity().get())) {
        return null;
      }
    }

    var db = ctx.getDatabaseSession();
    var result = new ResultInternal(db);
    for (var prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, next.getEntity().map(x -> toResult(db, x)).orElse(null));
    return result;
  }

  public static boolean isEmptyOptional(Object elem) {
    if (elem == EMPTY_OPTIONAL) {
      return true;
    }

    return elem instanceof Result && EMPTY_OPTIONAL == ((Result) elem).getEntity()
        .orElse(null);
  }
}
