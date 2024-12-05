package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OMatchPathItem;
import com.orientechnologies.core.sql.parser.ORid;
import com.orientechnologies.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class MatchEdgeTraverser {

  protected YTResult sourceRecord;
  protected EdgeTraversal edge;
  protected OMatchPathItem item;
  protected OExecutionStream downstream;

  public MatchEdgeTraverser(YTResult lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(YTResult lastUpstreamRecord, OMatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(OCommandContext ctx) {
    init(ctx);
    return downstream.hasNext(ctx);
  }

  public YTResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext(ctx)) {
      throw new IllegalStateException();
    }
    String endPointAlias = getEndpointAlias();
    YTResult nextR = downstream.next(ctx);
    YTIdentifiable nextElement = nextR.getEntity().get();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }

    var db = ctx.getDatabase();
    YTResultInternal result = new YTResultInternal(db);
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, toResult(db, nextElement));
    if (edge.edge.item.getFilter().getDepthAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getDepthAlias(), nextR.getMetadata("$depth"));
    }
    if (edge.edge.item.getFilter().getPathAlias() != null) {
      result.setProperty(
          edge.edge.item.getFilter().getPathAlias(), nextR.getMetadata("$matchPath"));
    }
    return result;
  }

  protected boolean equals(Object prevValue, YTIdentifiable nextElement) {
    if (prevValue instanceof YTResult) {
      prevValue = ((YTResult) prevValue).getEntity().orElse(null);
    }
    if (nextElement instanceof YTResult) {
      nextElement = ((YTResult) nextElement).getEntity().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected static Object toResult(YTDatabaseSessionInternal db, YTIdentifiable nextElement) {
    return new YTResultInternal(db, nextElement);
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof YTResult) {
        startingElem = ((YTResult) startingElem).getEntity().orElse(null);
      }
      downstream = executeTraversal(ctx, this.item, (YTIdentifiable) startingElem, 0, null);
    }
  }

  protected OExecutionStream executeTraversal(
      OCommandContext iCommandContext,
      OMatchPathItem item,
      YTIdentifiable startingPoint,
      int depth,
      List<YTIdentifiable> pathToHere) {

    OWhereClause filter = null;
    OWhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    ORid targetRid = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
      String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getClusterIdByName(clusterName);
      }
      targetRid = targetRid(item, iCommandContext);
    }

    if (whileCondition == null
        && maxDepth
        == null) { // in this case starting point is not returned and only one level depth is
      // evaluated

      OExecutionStream queryResult = traversePatternEdge(startingPoint, iCommandContext);
      final OWhereClause theFilter = filter;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final ORid theTargetRid = targetRid;
      return queryResult.filter(
          (next, ctx) ->
              filter(
                  iCommandContext, theFilter, theClassName, theClusterId, theTargetRid, next, ctx));
    } else { // in this case also zero level (starting point) is considered and traversal depth is
      // given by the while condition
      List<YTResult> result = new ArrayList<>();
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);

      if (matchesFilters(iCommandContext, filter, startingPoint)
          && matchesClass(iCommandContext, className, startingPoint)
          && matchesCluster(iCommandContext, clusterId, startingPoint)
          && matchesRid(iCommandContext, targetRid, startingPoint)) {
        YTResultInternal rs = new YTResultInternal(iCommandContext.getDatabase(), startingPoint);
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.EMPTY_LIST : pathToHere);
        // add the result to the list
        result.add(rs);
      }

      if ((maxDepth == null || depth < maxDepth)
          && (whileCondition == null
          || whileCondition.matchesFilters(startingPoint, iCommandContext))) {

        OExecutionStream queryResult = traversePatternEdge(startingPoint, iCommandContext);

        while (queryResult.hasNext(iCommandContext)) {
          YTResult origin = queryResult.next(iCommandContext);
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          List<YTIdentifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          YTEntity elem = origin.toEntity();
          newPath.add(elem.getIdentity());

          OExecutionStream subResult =
              executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          while (subResult.hasNext(iCommandContext)) {
            YTResult sub = subResult.next(iCommandContext);
            result.add(sub);
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
      return OExecutionStream.resultIterator(result.iterator());
    }
  }

  private YTResult filter(
      OCommandContext iCommandContext,
      final OWhereClause theFilter,
      final String theClassName,
      final Integer theClusterId,
      final ORid theTargetRid,
      YTResult next,
      OCommandContext ctx) {
    Object previousMatch = ctx.getVariable("$currentMatch");
    YTResultInternal matched = (YTResultInternal) ctx.getVariable("matched");
    if (matched != null) {
      matched.setProperty(
          getStartingPointAlias(), sourceRecord.getProperty(getStartingPointAlias()));
    }
    YTEntity elem = next.toEntity();
    iCommandContext.setVariable("$currentMatch", elem);
    if (matchesFilters(iCommandContext, theFilter, elem)
        && matchesClass(iCommandContext, theClassName, elem)
        && matchesCluster(iCommandContext, theClusterId, elem)
        && matchesRid(iCommandContext, theTargetRid, elem)) {
      ctx.setVariable("$currentMatch", previousMatch);
      return next;
    } else {
      ctx.setVariable("$currentMatch", previousMatch);
      return null;
    }
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClassName(iCommandContext);
  }

  protected String targetClusterName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClusterName(iCommandContext);
  }

  protected ORid targetRid(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(
      OCommandContext iCommandContext, String className, YTIdentifiable origin) {
    if (className == null) {
      return true;
    }
    YTEntity element = null;
    if (origin instanceof YTEntity) {
      element = (YTEntity) origin;
    } else if (origin != null) {
      Object record = origin.getRecord();
      if (record instanceof YTEntity) {
        element = (YTEntity) record;
      }
    }
    if (element != null) {
      Optional<YTClass> clazz = element.getSchemaType();
      if (!clazz.isPresent()) {
        return false;
      }
      return clazz.get().isSubClassOf(className);
    }
    return false;
  }

  private boolean matchesCluster(
      OCommandContext iCommandContext, Integer clusterId, YTIdentifiable origin) {
    if (clusterId == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return clusterId.equals(origin.getIdentity().getClusterId());
  }

  private boolean matchesRid(OCommandContext iCommandContext, ORid rid, YTIdentifiable origin) {
    if (rid == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return origin.getIdentity().equals(rid.toRecordId(origin, iCommandContext));
  }

  protected boolean matchesFilters(
      OCommandContext iCommandContext, OWhereClause filter, YTIdentifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  // TODO refactor this method to receive the item.

  protected OExecutionStream traversePatternEdge(
      YTIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      qR = this.item.getMethod().execute(startingPoint, iCommandContext);
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
