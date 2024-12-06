package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBatch;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
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

  private final SQLIdentifier targetClass;
  private final SQLIdentifier targetCluster;
  private final String uniqueIndexName;
  private final SQLIdentifier fromAlias;
  private final SQLIdentifier toAlias;
  private final Number wait;
  private final Number retry;
  private final SQLBatch batch;

  public CreateEdgesStep(
      SQLIdentifier targetClass,
      SQLIdentifier targetClusterName,
      String uniqueIndex,
      SQLIdentifier fromAlias,
      SQLIdentifier toAlias,
      Number wait,
      Number retry,
      SQLBatch batch,
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
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<?> fromIter = fetchFroms();
    List<Object> toList = fetchTo();
    Index uniqueIndex = findIndex(this.uniqueIndexName);
    Stream<Result> stream =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(fromIter, 0), false)
            .map(CreateEdgesStep::asVertex)
            .flatMap((currentFrom) -> mapTo(ctx.getDatabase(), toList, currentFrom, uniqueIndex));
    return ExecutionStream.resultIterator(stream.iterator());
  }

  private Index findIndex(String uniqueIndexName) {
    if (uniqueIndexName != null) {
      final DatabaseSessionInternal database = ctx.getDatabase();
      Index uniqueIndex =
          database.getMetadata().getIndexManagerInternal().getIndex(database, uniqueIndexName);
      if (uniqueIndex == null) {
        throw new CommandExecutionException("Index not found for upsert: " + uniqueIndexName);
      }
      return uniqueIndex;
    }
    return null;
  }

  private List<Object> fetchTo() {
    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof Identifiable)) {
      toValues = ((Iterable<?>) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }
    if (toValues instanceof InternalResultSet) {
      toValues = ((InternalResultSet) toValues).copy();
    }

    Iterator<?> toIter = (Iterator<?>) toValues;

    if (toIter instanceof ResultSet) {
      try {
        ((ResultSet) toIter).reset();
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
    if (fromValues instanceof Iterable && !(fromValues instanceof Identifiable)) {
      fromValues = ((Iterable<?>) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    if (fromValues instanceof InternalResultSet) {
      fromValues = ((InternalResultSet) fromValues).copy();
    }
    Iterator<?> fromIter = (Iterator<?>) fromValues;
    if (fromIter instanceof ResultSet) {
      try {
        ((ResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }
    return fromIter;
  }

  public Stream<Result> mapTo(DatabaseSessionInternal db, List<Object> to, Vertex currentFrom,
      Index uniqueIndex) {
    return to.stream()
        .map(
            (obj) -> {
              Vertex currentTo = asVertex(obj);
              if (currentTo == null) {
                throw new CommandExecutionException("Invalid TO vertex for edge");
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
                    throw new CommandExecutionException(
                        "Cannot set target cluster on lightweight edges");
                  }

                  edgeToUpdate.getBaseDocument().save(targetCluster.getStringValue());
                }
              }

              currentFrom.save();
              currentTo.save();
              edgeToUpdate.save();

              return new UpdatableResult(db, edgeToUpdate);
            });
  }

  private static EdgeInternal getExistingEdge(
      DatabaseSessionInternal session,
      Vertex currentFrom,
      Vertex currentTo,
      Index uniqueIndex) {
    Object key =
        uniqueIndex
            .getDefinition()
            .createValue(session, currentFrom.getIdentity(), currentTo.getIdentity());

    final Iterator<RID> iterator;
    try (Stream<RID> stream = uniqueIndex.getInternal().getRids(session, key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next().getRecord();
      }
    }

    return null;
  }

  private static Vertex asVertex(Object currentFrom) {
    if (currentFrom instanceof RID) {
      currentFrom = ((RID) currentFrom).getRecord();
    }
    if (currentFrom instanceof Result) {
      Object from = currentFrom;
      currentFrom =
          ((Result) currentFrom)
              .getVertex()
              .orElseThrow(
                  () ->
                      new CommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    if (currentFrom instanceof Vertex) {
      return (Vertex) currentFrom;
    }
    if (currentFrom instanceof Entity) {
      Object from = currentFrom;
      return ((Entity) currentFrom)
          .asVertex()
          .orElseThrow(
              () -> new CommandExecutionException("Invalid vertex for edge creation: " + from));
    }
    throw new CommandExecutionException(
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
