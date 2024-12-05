package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStreamProducer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OMultipleExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {

  private final String toAlias;
  private final OIdentifier targetCluster;
  private final OIdentifier targetClass;

  public FetchEdgesToVerticesStep(
      String toAlias,
      OIdentifier targetClass,
      OIdentifier targetCluster,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.toAlias = toAlias;
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Stream<Object> source = init();

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator iter = source.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            return edges(ctx.getDatabase(), iter.next());
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

  private Stream<Object> init() {
    Object toValues;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof YTIdentifiable)) {
      toValues = ((Iterable<?>) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize((Iterator<?>) toValues, 0), false);
  }

  private OExecutionStream edges(YTDatabaseSessionInternal db, Object from) {
    if (from instanceof YTResult) {
      from = ((YTResult) from).toEntity();
    }
    if (from instanceof YTIdentifiable && !(from instanceof Entity)) {
      from = ((YTIdentifiable) from).getRecord();
    }
    if (from instanceof Entity && ((Entity) from).isVertex()) {
      var vertex = ((Entity) from).toVertex();
      assert vertex != null;
      Iterable<Edge> edges = vertex.getEdges(ODirection.IN);
      Stream<YTResult> stream =
          StreamSupport.stream(edges.spliterator(), false)
              .filter((edge) -> matchesClass(edge) && matchesCluster(edge))
              .map(e -> new YTResultInternal(db, e));
      return OExecutionStream.resultIterator(stream.iterator());
    } else {
      throw new YTCommandExecutionException("Invalid vertex: " + from);
    }
  }

  private boolean matchesCluster(Edge edge) {
    if (targetCluster == null) {
      return true;
    }
    int clusterId = edge.getIdentity().getClusterId();
    String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
    return clusterName.equals(targetCluster.getStringValue());
  }

  private boolean matchesClass(Edge edge) {
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
    String result = spaces + "+ FOR EACH x in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES TO x";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }
}
