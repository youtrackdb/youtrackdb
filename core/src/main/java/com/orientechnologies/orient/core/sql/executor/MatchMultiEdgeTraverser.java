package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OMatchFilter;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItemFirst;
import com.orientechnologies.orient.core.sql.parser.OMethodCall;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class MatchMultiEdgeTraverser extends MatchEdgeTraverser {

  public MatchMultiEdgeTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected OExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    //    if (this.edge.edge.item.getFilter() != null) {
    //      String alias = this.edge.edge.item.getFilter().getAlias();
    //      Object matchedNodes =
    // iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    //      if (matchedNodes != null) {
    //        if (matchedNodes instanceof Iterable) {
    //          possibleResults = (Iterable) matchedNodes;
    //        } else {
    //          possibleResults = Collections.singleton(matchedNodes);
    //        }
    //      }
    //    }

    OMultiMatchPathItem item = (OMultiMatchPathItem) this.item;
    List<YTResult> result = new ArrayList<>();

    List<Object> nextStep = new ArrayList<>();
    nextStep.add(startingPoint);

    var db = iCommandContext.getDatabase();
    Object oldCurrent = iCommandContext.getVariable("$current");
    for (OMatchPathItem sub : item.getItems()) {
      List<YTResult> rightSide = new ArrayList<>();
      for (Object o : nextStep) {
        OWhereClause whileCond =
            sub.getFilter() == null ? null : sub.getFilter().getWhileCondition();

        OMethodCall method = sub.getMethod();
        if (sub instanceof OMatchPathItemFirst) {
          method = ((OMatchPathItemFirst) sub).getFunction().toMethod();
        }

        if (whileCond != null) {
          Object current = o;
          if (current instanceof YTResult) {
            current = ((YTResult) current).getElement().orElse(null);
          }
          MatchEdgeTraverser subtraverser = new MatchEdgeTraverser(null, sub);
          OExecutionStream rightStream =
              subtraverser.executeTraversal(iCommandContext, sub, (YTIdentifiable) current, 0,
                  null);
          while (rightStream.hasNext(iCommandContext)) {
            rightSide.add(rightStream.next(iCommandContext));
          }

        } else {
          iCommandContext.setVariable("$current", o);
          Object nextSteps = method.execute(o, possibleResults, iCommandContext);
          if (nextSteps instanceof Collection) {
            ((Collection<?>) nextSteps)
                .stream()
                .map(obj -> toOResultInternal(db, obj))
                .filter(
                    x ->
                        matchesCondition(x, sub.getFilter(), iCommandContext))
                .forEach(i -> rightSide.add(i));
          } else if (nextSteps instanceof YTIdentifiable) {
            YTResultInternal res = new YTResultInternal(db, (YTIdentifiable) nextSteps);
            if (matchesCondition(res, sub.getFilter(), iCommandContext)) {
              rightSide.add(res);
            }
          } else if (nextSteps instanceof YTResultInternal) {
            if (matchesCondition((YTResultInternal) nextSteps, sub.getFilter(), iCommandContext)) {
              rightSide.add((YTResultInternal) nextSteps);
            }
          } else if (nextSteps instanceof Iterable) {
            for (Object step : (Iterable) nextSteps) {
              YTResultInternal converted = toOResultInternal(db, step);
              if (matchesCondition(converted, sub.getFilter(), iCommandContext)) {
                rightSide.add(converted);
              }
            }
          } else if (nextSteps instanceof Iterator iterator) {
            while (iterator.hasNext()) {
              YTResultInternal converted = toOResultInternal(db, iterator.next());
              if (matchesCondition(converted, sub.getFilter(), iCommandContext)) {
                rightSide.add(converted);
              }
            }
          }
        }
      }
      nextStep = (List) rightSide;
      result = rightSide;
    }

    iCommandContext.setVariable("$current", oldCurrent);
    //    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((YTIdentifiable)
    // qR);
    return OExecutionStream.resultIterator(result.iterator());
  }

  private boolean matchesCondition(YTResultInternal x, OMatchFilter filter, OCommandContext ctx) {
    if (filter == null) {
      return true;
    }
    OWhereClause where = filter.getFilter();
    if (where == null) {
      return true;
    }
    return where.matchesFilters(x, ctx);
  }

  private static YTResultInternal toOResultInternal(YTDatabaseSessionInternal db, Object x) {
    if (x instanceof YTResultInternal) {
      return (YTResultInternal) x;
    }
    if (x instanceof YTIdentifiable) {
      return new YTResultInternal(db, (YTIdentifiable) x);
    }
    throw new YTCommandExecutionException("Cannot execute traversal on " + x);
  }
}
