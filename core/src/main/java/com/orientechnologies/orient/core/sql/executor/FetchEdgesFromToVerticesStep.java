package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStreamProducer;
import com.orientechnologies.orient.core.sql.executor.resultset.OMultipleExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 *
 */
public class FetchEdgesFromToVerticesStep extends AbstractExecutionStep {

  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final String fromAlias;
  private final String toAlias;

  public FetchEdgesFromToVerticesStep(
      String fromAlias,
      String toAlias,
      OIdentifier targetClass,
      OIdentifier targetCluster,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    final Iterator fromIter = loadFrom();

    final Set<YTRID> toList = loadTo();

    var db = ctx.getDatabase();
    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator iter = fromIter;
          private final Set<YTRID> to = toList;

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            return createResultSet(db, to, iter.next());
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

  private OExecutionStream createResultSet(YTDatabaseSessionInternal db, Set<YTRID> toList,
      Object val) {
    return OExecutionStream.resultIterator(
        StreamSupport.stream(this.loadNextResults(val).spliterator(), false)
            .filter((e) -> filterResult(e, toList))
            .map(
                (edge) -> {
                  return (YTResult) new YTResultInternal(db, edge);
                })
            .iterator());
  }

  private Set<YTRID> loadTo() {
    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof YTIdentifiable)) {
      toValues = ((Iterable<?>) toValues).iterator();
    } else if (!(toValues instanceof Iterator) && toValues != null) {
      toValues = Collections.singleton(toValues).iterator();
    }

    Iterator<?> toIter = (Iterator<?>) toValues;
    if (toIter != null) {
      final Set<YTRID> toList = new HashSet<YTRID>();
      while (toIter.hasNext()) {
        Object elem = toIter.next();
        if (elem instanceof YTResult) {
          elem = ((YTResult) elem).toEntity();
        }
        if (elem instanceof YTIdentifiable && !(elem instanceof YTEntity)) {
          elem = ((YTIdentifiable) elem).getRecord();
        }
        if (!(elem instanceof YTEntity)) {
          throw new YTCommandExecutionException("Invalid vertex: " + elem);
        }
        ((YTEntity) elem).asVertex().ifPresent(x -> toList.add(x.getIdentity()));
      }

      return toList;
    }
    return null;
  }

  private Iterator<?> loadFrom() {
    Object fromValues = null;

    fromValues = ctx.getVariable(fromAlias);
    if (fromValues instanceof Iterable && !(fromValues instanceof YTIdentifiable)) {
      fromValues = ((Iterable<?>) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    return (Iterator<?>) fromValues;
  }

  private boolean filterResult(YTEdge edge, Set<YTRID> toList) {
    if (toList == null || toList.contains(edge.getTo().getIdentity())) {
      return matchesClass(edge) && matchesCluster(edge);
    }
    return true;
  }

  private Iterable<YTEdge> loadNextResults(Object from) {
    if (from instanceof YTResult) {
      from = ((YTResult) from).toEntity();
    }
    if (from instanceof YTIdentifiable && !(from instanceof YTEntity)) {
      from = ((YTIdentifiable) from).getRecord();
    }
    if (from instanceof YTEntity && ((YTEntity) from).isVertex()) {
      var vertex = ((YTEntity) from).toVertex();
      assert vertex != null;
      return vertex.getEdges(ODirection.OUT);
    } else {
      throw new YTCommandExecutionException("Invalid vertex: " + from);
    }
  }

  private boolean matchesCluster(YTEdge edge) {
    if (targetCluster == null) {
      return true;
    }
    int clusterId = edge.getIdentity().getClusterId();
    String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
    return clusterName.equals(targetCluster.getStringValue());
  }

  private boolean matchesClass(YTEdge edge) {
    if (targetClass == null) {
      return true;
    }
    var schemaClass = edge.getSchemaClass();
    assert schemaClass != null;
    return schemaClass.isSubClassOf(targetClass.getStringValue());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES FROM x TO y";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchEdgesFromToVerticesStep(
        fromAlias, toAlias, targetClass, targetCluster, ctx, profilingEnabled);
  }
}
