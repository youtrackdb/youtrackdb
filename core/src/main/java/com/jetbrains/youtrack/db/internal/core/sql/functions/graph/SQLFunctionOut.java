package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.common.collection.MultiCollectionIterator;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class SQLFunctionOut extends SQLFunctionMoveFiltered {

  public static final String NAME = "out";

  public SQLFunctionOut() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final DatabaseSessionInternal graph, final Identifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, Direction.OUT, iLabels);
  }

  protected Object move(
      final DatabaseSessionInternal graph,
      final Identifiable iRecord,
      final String[] iLabels,
      Iterable<Identifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, Direction.OUT, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    var edges = v2e(graph, iRecord, Direction.OUT, iLabels);
    if (edges instanceof Sizeable) {
      var size = ((Sizeable) edges).size();
      if (size > supernodeThreshold) {
        var result = fetchFromIndex(graph, iRecord, iPossibleResults, iLabels);
        if (result != null) {
          return result;
        }
      }
    }

    return v2v(graph, iRecord, Direction.OUT, iLabels);
  }

  private static Object fetchFromIndex(
      DatabaseSessionInternal graph,
      Identifiable iFrom,
      Iterable<Identifiable> iTo,
      String[] iEdgeTypes) {
    String edgeClassName = null;
    if (iEdgeTypes == null) {
      edgeClassName = "E";
    } else if (iEdgeTypes.length == 1) {
      edgeClassName = iEdgeTypes[0];
    } else {
      return null;
    }
    var edgeClass =
        ((DatabaseSessionInternal) graph)
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClassInternal(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    var indexes = edgeClass.getInvolvedIndexesInternal(graph, "out", "in");
    if (indexes == null || indexes.isEmpty()) {
      return null;
    }
    var index = indexes.iterator().next();

    var result = new MultiCollectionIterator<Vertex>();
    for (var to : iTo) {
      final var key = new CompositeKey(iFrom, to);
      try (var stream = index.getInternal()
          .getRids(graph, key)) {
        result.add(
            stream
                .map((rid) -> ((EntityImpl) rid.getRecord(graph)).rawField("in"))
                .collect(Collectors.toSet()));
      }
    }

    return result;
  }
}
