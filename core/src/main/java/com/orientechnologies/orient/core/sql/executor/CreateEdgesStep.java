package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTEdgeInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBatch;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public class CreateEdgesStep extends AbstractExecutionStep {

  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final String uniqueIndexName;
  private final OIdentifier fromAlias;
  private final OIdentifier toAlias;
  private final Number wait;
  private final Number retry;
  private final OBatch batch;

  public CreateEdgesStep(
      OIdentifier targetClass,
      OIdentifier targetClusterName,
      String uniqueIndex,
      OIdentifier fromAlias,
      OIdentifier toAlias,
      Number wait,
      Number retry,
      OBatch batch,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetClusterName;
    this.uniqueIndexName = uniqueIndex;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
    this.wait = wait;
    this.retry = retry;
    this.batch = batch;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<?> fromIter = fetchFroms();
    List<Object> toList = fetchTo();
    OIndex uniqueIndex = findIndex(this.uniqueIndexName);
    Stream<OResult> stream =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(fromIter, 0), false)
            .map(CreateEdgesStep::asVertex)
            .flatMap((currentFrom) -> mapTo(ctx.getDatabase(), toList, currentFrom, uniqueIndex));
    return OExecutionStream.resultIterator(stream.iterator());
  }

  private OIndex findIndex(String uniqueIndexName) {
    if (uniqueIndexName != null) {
      final YTDatabaseSessionInternal database = ctx.getDatabase();
      OIndex uniqueIndex =
          database.getMetadata().getIndexManagerInternal().getIndex(database, uniqueIndexName);
      if (uniqueIndex == null) {
        throw new OCommandExecutionException("Index not found for upsert: " + uniqueIndexName);
      }
      return uniqueIndex;
    }
    return null;
  }

  private List<Object> fetchTo() {
    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof YTIdentifiable)) {
      toValues = ((Iterable<?>) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }
    if (toValues instanceof OInternalResultSet) {
      toValues = ((OInternalResultSet) toValues).copy();
    }

    Iterator<?> toIter = (Iterator<?>) toValues;

    if (toIter instanceof OResultSet) {
      try {
        ((OResultSet) toIter).reset();
      } catch (Exception ignore) {
      }
    }
    List<Object> toList = new ArrayList<>();
    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }
    return toList;
  }

  private Iterator<?> fetchFroms() {
    Object fromValues = ctx.getVariable(fromAlias.getStringValue());
    if (fromValues instanceof Iterable && !(fromValues instanceof YTIdentifiable)) {
      fromValues = ((Iterable<?>) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    if (fromValues instanceof OInternalResultSet) {
      fromValues = ((OInternalResultSet) fromValues).copy();
    }
    Iterator<?> fromIter = (Iterator<?>) fromValues;
    if (fromIter instanceof OResultSet) {
      try {
        ((OResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }
    return fromIter;
  }

  public Stream<OResult> mapTo(YTDatabaseSessionInternal db, List<Object> to, YTVertex currentFrom,
      OIndex uniqueIndex) {
    return to.stream()
        .map(
            (obj) -> {
              YTVertex currentTo = asVertex(obj);
              if (currentTo == null) {
                throw new OCommandExecutionException("Invalid TO vertex for edge");
              }
              YTEdgeInternal edgeToUpdate = null;
              if (uniqueIndex != null) {
                YTEdgeInternal existingEdge =
                    getExistingEdge(ctx.getDatabase(), currentFrom, currentTo, uniqueIndex);
                if (existingEdge != null) {
                  edgeToUpdate = existingEdge;
                }
              }

              if (edgeToUpdate == null) {
                edgeToUpdate =
                    (YTEdgeInternal) currentFrom.addEdge(currentTo, targetClass.getStringValue());
                if (targetCluster != null) {
                  if (edgeToUpdate.isLightweight()) {
                    throw new OCommandExecutionException(
                        "Cannot set target cluster on lightweight edges");
                  }

                  edgeToUpdate.getBaseDocument().save(targetCluster.getStringValue());
                }
              }

              currentFrom.save();
              currentTo.save();
              edgeToUpdate.save();

              return new OUpdatableResult(db, edgeToUpdate);
            });
  }

  private static YTEdgeInternal getExistingEdge(
      YTDatabaseSessionInternal session,
      YTVertex currentFrom,
      YTVertex currentTo,
      OIndex uniqueIndex) {
    Object key =
        uniqueIndex
            .getDefinition()
            .createValue(session, currentFrom.getIdentity(), currentTo.getIdentity());

    final Iterator<YTRID> iterator;
    try (Stream<YTRID> stream = uniqueIndex.getInternal().getRids(session, key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next().getRecord();
      }
    }

    return null;
  }

  private static YTVertex asVertex(Object currentFrom) {
    if (currentFrom instanceof YTRID) {
      currentFrom = ((YTRID) currentFrom).getRecord();
    }
    if (currentFrom instanceof OResult) {
      Object from = currentFrom;
      currentFrom =
          ((OResult) currentFrom)
              .getVertex()
              .orElseThrow(
                  () ->
                      new OCommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    if (currentFrom instanceof YTVertex) {
      return (YTVertex) currentFrom;
    }
    if (currentFrom instanceof YTEntity) {
      Object from = currentFrom;
      return ((YTEntity) currentFrom)
          .asVertex()
          .orElseThrow(
              () -> new OCommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    throw new OCommandExecutionException(
        "Invalid vertex for edge creation: "
            + (currentFrom == null ? "null" : currentFrom.toString()));
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       CREATE EDGE " + targetClass + " FROM x TO y";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
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
    return new CreateEdgesStep(
        targetClass == null ? null : targetClass.copy(),
        targetCluster == null ? null : targetCluster.copy(),
        uniqueIndexName,
        fromAlias == null ? null : fromAlias.copy(),
        toAlias == null ? null : toAlias.copy(),
        wait,
        retry,
        batch == null ? null : batch.copy(),
        ctx,
        profilingEnabled);
  }
}
