package com.orientechnologies.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OCompositeKey;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.YTVertex;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class OSQLFunctionOut extends OSQLFunctionMoveFiltered {

  public static final String NAME = "out";

  public OSQLFunctionOut() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final YTDatabaseSession graph, final YTIdentifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, ODirection.OUT, iLabels);
  }

  protected Object move(
      final YTDatabaseSession graph,
      final YTIdentifiable iRecord,
      final String[] iLabels,
      Iterable<YTIdentifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, ODirection.OUT, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    Object edges = v2e(graph, iRecord, ODirection.OUT, iLabels);
    if (edges instanceof OSizeable) {
      int size = ((OSizeable) edges).size();
      if (size > supernodeThreshold) {
        Object result = fetchFromIndex(graph, iRecord, iPossibleResults, iLabels);
        if (result != null) {
          return result;
        }
      }
    }

    return v2v(graph, iRecord, ODirection.OUT, iLabels);
  }

  private Object fetchFromIndex(
      YTDatabaseSession graph,
      YTIdentifiable iFrom,
      Iterable<YTIdentifiable> iTo,
      String[] iEdgeTypes) {
    String edgeClassName = null;
    if (iEdgeTypes == null) {
      edgeClassName = "E";
    } else if (iEdgeTypes.length == 1) {
      edgeClassName = iEdgeTypes[0];
    } else {
      return null;
    }
    YTClass edgeClass =
        ((YTDatabaseSessionInternal) graph)
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    Set<OIndex> indexes = edgeClass.getInvolvedIndexes(graph, "out", "in");
    if (indexes == null || indexes.isEmpty()) {
      return null;
    }
    OIndex index = indexes.iterator().next();

    OMultiCollectionIterator<YTVertex> result = new OMultiCollectionIterator<YTVertex>();
    for (YTIdentifiable to : iTo) {
      final OCompositeKey key = new OCompositeKey(iFrom, to);
      try (Stream<YTRID> stream = index.getInternal()
          .getRids((YTDatabaseSessionInternal) graph, key)) {
        result.add(
            stream
                .map((rid) -> ((YTEntityImpl) rid.getRecord()).rawField("in"))
                .collect(Collectors.toSet()));
      }
    }

    return result;
  }
}
