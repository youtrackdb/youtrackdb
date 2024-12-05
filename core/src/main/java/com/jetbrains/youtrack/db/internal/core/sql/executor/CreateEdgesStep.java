package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBatch;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
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
      CommandContext ctx,
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
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<?> fromIter = fetchFroms();
    List<Object> toList = fetchTo();
    OIndex uniqueIndex = findIndex(this.uniqueIndexName);
    Stream<YTResult> stream =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(fromIter, 0), false)
            .map(CreateEdgesStep::asVertex)
            .flatMap((currentFrom) -> mapTo(ctx.getDatabase(), toList, currentFrom, uniqueIndex));
    return ExecutionStream.resultIterator(stream.iterator());
  }

  private OIndex findIndex(String uniqueIndexName) {
    if (uniqueIndexName != null) {
      final YTDatabaseSessionInternal database = ctx.getDatabase();
      OIndex uniqueIndex =
          database.getMetadata().getIndexManagerInternal().getIndex(database, uniqueIndexName);
      if (uniqueIndex == null) {
        throw new YTCommandExecutionException("Index not found for upsert: " + uniqueIndexName);
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
    if (toValues instanceof YTInternalResultSet) {
      toValues = ((YTInternalResultSet) toValues).copy();
    }

    Iterator<?> toIter = (Iterator<?>) toValues;

    if (toIter instanceof YTResultSet) {
      try {
        ((YTResultSet) toIter).reset();
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
    if (fromValues instanceof YTInternalResultSet) {
      fromValues = ((YTInternalResultSet) fromValues).copy();
    }
    Iterator<?> fromIter = (Iterator<?>) fromValues;
    if (fromIter instanceof YTResultSet) {
      try {
        ((YTResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }
    return fromIter;
  }

  public Stream<YTResult> mapTo(YTDatabaseSessionInternal db, List<Object> to, Vertex currentFrom,
      OIndex uniqueIndex) {
    return to.stream()
        .map(
            (obj) -> {
              Vertex currentTo = asVertex(obj);
              if (currentTo == null) {
                throw new YTCommandExecutionException("Invalid TO vertex for edge");
              }
              EdgeInternal edgeToUpdate = null;
              if (uniqueIndex != null) {
                EdgeInternal existingEdge =
                    getExistingEdge(ctx.getDatabase(), currentFrom, currentTo, uniqueIndex);
                if (existingEdge != null) {
                  edgeToUpdate = existingEdge;
                }
              }

              if (edgeToUpdate == null) {
                edgeToUpdate =
                    (EdgeInternal) currentFrom.addEdge(currentTo, targetClass.getStringValue());
                if (targetCluster != null) {
                  if (edgeToUpdate.isLightweight()) {
                    throw new YTCommandExecutionException(
                        "Cannot set target cluster on lightweight edges");
                  }

                  edgeToUpdate.getBaseDocument().save(targetCluster.getStringValue());
                }
              }

              currentFrom.save();
              currentTo.save();
              edgeToUpdate.save();

              return new YTUpdatableResult(db, edgeToUpdate);
            });
  }

  private static EdgeInternal getExistingEdge(
      YTDatabaseSessionInternal session,
      Vertex currentFrom,
      Vertex currentTo,
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

  private static Vertex asVertex(Object currentFrom) {
    if (currentFrom instanceof YTRID) {
      currentFrom = ((YTRID) currentFrom).getRecord();
    }
    if (currentFrom instanceof YTResult) {
      Object from = currentFrom;
      currentFrom =
          ((YTResult) currentFrom)
              .getVertex()
              .orElseThrow(
                  () ->
                      new YTCommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    if (currentFrom instanceof Vertex) {
      return (Vertex) currentFrom;
    }
    if (currentFrom instanceof Entity) {
      Object from = currentFrom;
      return ((Entity) currentFrom)
          .asVertex()
          .orElseThrow(
              () -> new YTCommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    throw new YTCommandExecutionException(
        "Invalid vertex for edge creation: "
            + (currentFrom == null ? "null" : currentFrom.toString()));
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
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
  public ExecutionStep copy(CommandContext ctx) {
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
