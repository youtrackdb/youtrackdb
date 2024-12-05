package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 *
 */
public class OptionalMatchEdgeTraverser extends MatchEdgeTraverser {

  public static final YTResult EMPTY_OPTIONAL = new YTResultInternal(null);

  public OptionalMatchEdgeTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      super.init(ctx);
      if (!downstream.hasNext(ctx)) {
        downstream = OExecutionStream.singleton(EMPTY_OPTIONAL);
      }
    }
  }

  public YTResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext(ctx)) {
      throw new IllegalStateException();
    }

    String endPointAlias = getEndpointAlias();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    YTResult next = downstream.next(ctx);

    if (isEmptyOptional(prevValue)) {
      return sourceRecord;
    }
    if (!isEmptyOptional(next)) {
      if (prevValue != null && !equals(prevValue, next.getEntity().get())) {
        return null;
      }
    }

    var db = ctx.getDatabase();
    YTResultInternal result = new YTResultInternal(db);
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, next.getEntity().map(x -> toResult(db, x)).orElse(null));
    return result;
  }

  public static boolean isEmptyOptional(Object elem) {
    if (elem == EMPTY_OPTIONAL) {
      return true;
    }

    return elem instanceof YTResult && EMPTY_OPTIONAL == ((YTResult) elem).getEntity()
        .orElse(null);
  }
}
