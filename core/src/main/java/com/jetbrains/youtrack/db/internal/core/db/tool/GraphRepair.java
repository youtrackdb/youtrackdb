package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.metadata.Metadata;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.VertexInternal;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.StorageRecoverEventListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Repairs a graph. Current implementation scan the entire graph. In the future the WAL will be used
 * to make this repair task much faster.
 */
public class GraphRepair {

  private class RepairStats {

    private long scannedEdges = 0;
    private long removedEdges = 0;
    private long scannedVertices = 0;
    private long scannedLinks = 0;
    private long removedLinks = 0;
    private long repairedVertices = 0;
  }

  private StorageRecoverEventListener eventListener;

  public void repair(
      final DatabaseSession graph,
      final CommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(outputListener, "Repair of graph '" + graph.getURL() + "' is started ...\n");

    final var beginTime = System.currentTimeMillis();

    final var stats = new RepairStats();

    // SCAN AND CLEAN ALL THE EDGES FIRST (IF ANY)
    repairEdges(graph, stats, outputListener, options, false);

    // SCAN ALL THE VERTICES
    repairVertices(graph, stats, outputListener, options, false);

    message(
        outputListener,
        "Repair of graph '"
            + graph.getURL()
            + "' completed in "
            + ((System.currentTimeMillis() - beginTime) / 1000)
            + " secs\n");

    message(outputListener, " scannedEdges.....: " + stats.scannedEdges + "\n");
    message(outputListener, " removedEdges.....: " + stats.removedEdges + "\n");
    message(outputListener, " scannedVertices..: " + stats.scannedVertices + "\n");
    message(outputListener, " scannedLinks.....: " + stats.scannedLinks + "\n");
    message(outputListener, " removedLinks.....: " + stats.removedLinks + "\n");
    message(outputListener, " repairedVertices.: " + stats.repairedVertices + "\n");
  }

  public void check(
      final DatabaseSession graph,
      final CommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(outputListener, "Check of graph '" + graph.getURL() + "' is started...\n");

    final var beginTime = System.currentTimeMillis();

    final var stats = new RepairStats();

    // SCAN AND CLEAN ALL THE EDGES FIRST (IF ANY)
    repairEdges(graph, stats, outputListener, options, true);

    // SCAN ALL THE VERTICES
    repairVertices(graph, stats, outputListener, options, true);

    message(
        outputListener,
        "Check of graph '"
            + graph.getURL()
            + "' completed in "
            + ((System.currentTimeMillis() - beginTime) / 1000)
            + " secs\n");

    message(outputListener, " scannedEdges.....: " + stats.scannedEdges + "\n");
    message(outputListener, " edgesToRemove....: " + stats.removedEdges + "\n");
    message(outputListener, " scannedVertices..: " + stats.scannedVertices + "\n");
    message(outputListener, " scannedLinks.....: " + stats.scannedLinks + "\n");
    message(outputListener, " linksToRemove....: " + stats.removedLinks + "\n");
    message(outputListener, " verticesToRepair.: " + stats.repairedVertices + "\n");
  }

  protected void repairEdges(
      final DatabaseSession graph,
      final RepairStats stats,
      final CommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final var session = (DatabaseSessionInternal) graph;
    session.executeInTx(
        () -> {
          final Metadata metadata = session.getMetadata();
          final Schema schema = metadata.getSchema();

          final var useVertexFieldsForEdgeLabels = true; // db.isUseVertexFieldsForEdgeLabels();

          final var edgeClass = schema.getClass(SchemaClass.EDGE_CLASS_NAME);
          if (edgeClass != null) {
            final var countEdges = session.countClass(edgeClass.getName(session));

            var skipEdges = 0L;
            if (options != null && options.get("-skipEdges") != null) {
              skipEdges = Long.parseLong(options.get("-skipEdges").get(0));
            }

            message(
                outputListener,
                "Scanning " + countEdges + " edges (skipEdges=" + skipEdges + ")...\n");

            var parsedEdges = 0L;
            final var beginTime = System.currentTimeMillis();

            for (var edge : session.browseClass(edgeClass.getName(session))) {
              if (!edge.isStatefulEdge()) {
                continue;
              }
              final RID edgeId = edge.getIdentity();

              parsedEdges++;
              if (skipEdges > 0 && parsedEdges <= skipEdges) {
                continue;
              }

              stats.scannedEdges++;

              if (eventListener != null) {
                eventListener.onScannedEdge(edge);
              }

              if (outputListener != null && stats.scannedEdges % 100000 == 0) {
                var speedPerSecond =
                    (long) (parsedEdges / ((System.currentTimeMillis() - beginTime) / 1000.0));
                if (speedPerSecond < 1) {
                  speedPerSecond = 1;
                }
                final var remaining = (countEdges - parsedEdges) / speedPerSecond;

                message(
                    outputListener,
                    "+ edges: scanned "
                        + stats.scannedEdges
                        + ", removed "
                        + stats.removedEdges
                        + " (estimated remaining time "
                        + remaining
                        + " secs)\n");
              }

              var outVertexMissing = false;

              var removalReason = "";

              final Identifiable out = edge.castToStatefulEdge().getFrom();
              if (out == null) {
                outVertexMissing = true;
              } else {
                EntityImpl outVertex;
                try {
                  outVertex = out.getRecord(session);
                } catch (RecordNotFoundException e) {
                  outVertex = null;
                }

                if (outVertex == null) {
                  outVertexMissing = true;
                } else {
                  final var outFieldName =
                      VertexInternal.getEdgeLinkFieldName(
                          Direction.OUT, edge.getSchemaClassName(), useVertexFieldsForEdgeLabels);

                  final var outEdges = outVertex.getPropertyInternal(outFieldName);
                  if (outEdges == null) {
                    outVertexMissing = true;
                  } else if (outEdges instanceof RidBag) {
                    if (!((RidBag) outEdges).contains(edgeId)) {
                      outVertexMissing = true;
                    }
                  } else if (outEdges instanceof Collection) {
                    if (!((Collection) outEdges).contains(edgeId)) {
                      outVertexMissing = true;
                    }
                  } else if (outEdges instanceof Identifiable) {
                    if (((Identifiable) outEdges).getIdentity().equals(edgeId)) {
                      outVertexMissing = true;
                    }
                  }
                }
              }

              if (outVertexMissing) {
                removalReason = "outgoing vertex (" + out + ") does not contain the edge";
              }

              var inVertexMissing = false;

              final Identifiable in = edge.castToStatefulEdge().getTo();
              if (in == null) {
                inVertexMissing = true;
              } else {

                EntityImpl inVertex;
                try {
                  inVertex = in.getRecord(session);
                } catch (RecordNotFoundException e) {
                  inVertex = null;
                }

                if (inVertex == null) {
                  inVertexMissing = true;
                } else {
                  final var inFieldName =
                      VertexInternal.getEdgeLinkFieldName(
                          Direction.IN, edge.getSchemaClassName(), useVertexFieldsForEdgeLabels);

                  final var inEdges = inVertex.getPropertyInternal(inFieldName);
                  if (inEdges == null) {
                    inVertexMissing = true;
                  } else if (inEdges instanceof RidBag) {
                    if (!((RidBag) inEdges).contains(edgeId)) {
                      inVertexMissing = true;
                    }
                  } else if (inEdges instanceof Collection) {
                    if (!((Collection) inEdges).contains(edgeId)) {
                      inVertexMissing = true;
                    }
                  } else if (inEdges instanceof Identifiable) {
                    if (((Identifiable) inEdges).getIdentity().equals(edgeId)) {
                      inVertexMissing = true;
                    }
                  }
                }
              }

              if (inVertexMissing) {
                if (!removalReason.isEmpty()) {
                  removalReason += ", ";
                }
                removalReason += "incoming vertex (" + in + ") does not contain the edge";
              }

              if (outVertexMissing || inVertexMissing) {
                try {
                  if (!checkOnly) {
                    message(
                        outputListener,
                        "+ deleting corrupted edge " + edge + " because " + removalReason + "\n");
                    edge.delete();
                  } else {
                    message(
                        outputListener,
                        "+ found corrupted edge " + edge + " because " + removalReason + "\n");
                  }

                  stats.removedEdges++;
                  if (eventListener != null) {
                    eventListener.onRemovedEdge(edge);
                  }

                } catch (Exception e) {
                  message(
                      outputListener,
                      "Error on deleting edge " + edge.getIdentity() + " (" + e.getMessage() + ")");
                }
              }
            }

            message(outputListener, "Scanning edges completed\n");
          }
        });
  }

  protected void repairVertices(
      final DatabaseSession session,
      final RepairStats stats,
      final CommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final var db = (DatabaseSessionInternal) session;
    final Metadata metadata = db.getMetadata();
    final Schema schema = metadata.getSchema();

    final var vertexClass = schema.getClass(SchemaClass.VERTEX_CLASS_NAME);
    if (vertexClass != null) {
      final var countVertices = db.countClass(vertexClass.getName(session));
      session.executeInTx(
          () -> {
            var skipVertices = 0L;
            if (options != null && options.get("-skipVertices") != null) {
              skipVertices = Long.parseLong(options.get("-skipVertices").get(0));
            }

            message(outputListener, "Scanning " + countVertices + " vertices...\n");

            var parsedVertices = new long[]{0L};
            final var beginTime = System.currentTimeMillis();

            for (var vertex : db.browseClass(vertexClass.getName(session))) {
              parsedVertices[0]++;
              if (skipVertices > 0 && parsedVertices[0] <= skipVertices) {
                continue;
              }

              var vertexCorrupted = false;
              stats.scannedVertices++;
              if (eventListener != null) {
                eventListener.onScannedVertex(vertex);
              }

              if (outputListener != null && stats.scannedVertices % 100000 == 0) {
                var speedPerSecond =
                    (long)
                        (parsedVertices[0] / ((System.currentTimeMillis() - beginTime) / 1000.0));
                if (speedPerSecond < 1) {
                  speedPerSecond = 1;
                }
                final var remaining = (countVertices - parsedVertices[0]) / speedPerSecond;

                message(
                    outputListener,
                    "+ vertices: scanned "
                        + stats.scannedVertices
                        + ", repaired "
                        + stats.repairedVertices
                        + " (estimated remaining time "
                        + remaining
                        + " secs)\n");
              }

              if (!vertex.isVertex()) {
                return;
              }

              for (var fieldName : vertex.fieldNames()) {
                final var connection =
                    VertexInternal.getConnection(db,
                        db.getMetadata().getSchema(), Direction.BOTH, fieldName);
                if (connection == null) {
                  continue;
                }

                final var fieldValue = vertex.rawField(fieldName);
                if (fieldValue != null) {
                  if (fieldValue instanceof Identifiable) {

                    if (isEdgeBroken(db,
                        vertex,
                        fieldName,
                        connection.getKey(),
                        (Identifiable) fieldValue,
                        stats, true)) {
                      vertexCorrupted = true;
                      if (!checkOnly) {
                        vertex.field(fieldName, null);
                      } else {
                        message(
                            outputListener,
                            "+ found corrupted vertex "
                                + vertex
                                + " the property "
                                + fieldName
                                + " could be removed\n");
                      }
                    }

                  } else if (fieldValue instanceof Collection<?> coll) {

                    for (var it = coll.iterator(); it.hasNext(); ) {
                      final var o = it.next();

                      if (isEdgeBroken(db,
                          vertex, fieldName, connection.getKey(), (Identifiable) o,
                          stats, true)) {
                        vertexCorrupted = true;
                        if (!checkOnly) {
                          it.remove();
                        } else {
                          message(
                              outputListener,
                              "+ found corrupted vertex "
                                  + vertex
                                  + " the edge should be removed from property "
                                  + fieldName
                                  + " (collection)\n");
                        }
                      }
                    }

                  } else if (fieldValue instanceof RidBag ridbag) {
                    // In case of ridbags force save for trigger eventual conversions
                    if (ridbag.size() == 0) {
                      vertex.removeField(fieldName);
                    } else if (!ridbag.isEmbedded()
                        && ridbag.size()
                        < GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD
                        .getValueAsInteger()) {
                      vertex.setDirty();
                    }
                    for (Iterator<?> it = ridbag.iterator(); it.hasNext(); ) {
                      final var o = it.next();
                      if (isEdgeBroken(db,
                          vertex, fieldName, connection.getKey(), (Identifiable) o,
                          stats, true)) {
                        vertexCorrupted = true;
                        if (!checkOnly) {
                          it.remove();
                        } else {
                          message(
                              outputListener,
                              "+ found corrupted vertex "
                                  + vertex
                                  + " the edge should be removed from property "
                                  + fieldName
                                  + " (ridbag)\n");
                        }
                      }
                    }
                  }
                }
              }

              if (vertexCorrupted) {
                stats.repairedVertices++;
                if (eventListener != null) {
                  eventListener.onRepairedVertex(vertex);
                }

                message(outputListener, "+ repaired corrupted vertex " + vertex + "\n");
                if (!checkOnly) {

                }
              } else if (vertex.isDirty() && !checkOnly) {
                message(outputListener, "+ optimized vertex " + vertex + "\n");

              }
            }

            message(outputListener, "Scanning vertices completed\n");
          });
    }
  }

  private void onScannedLink(final RepairStats stats, final Identifiable fieldValue) {
    stats.scannedLinks++;
    if (eventListener != null) {
      eventListener.onScannedLink(fieldValue);
    }
  }

  private void onRemovedLink(final RepairStats stats, final Identifiable fieldValue) {
    stats.removedLinks++;
    if (eventListener != null) {
      eventListener.onRemovedLink(fieldValue);
    }
  }

  public StorageRecoverEventListener getEventListener() {
    return eventListener;
  }

  public GraphRepair setEventListener(final StorageRecoverEventListener eventListener) {
    this.eventListener = eventListener;
    return this;
  }

  private void message(final CommandOutputListener outputListener, final String message) {
    if (outputListener != null) {
      outputListener.onMessage(message);
    }
  }

  private boolean isEdgeBroken(
      DatabaseSessionInternal session, final Identifiable vertex,
      final String fieldName,
      final Direction direction,
      final Identifiable edgeRID,
      final RepairStats stats,
      final boolean useVertexFieldsForEdgeLabels) {
    onScannedLink(stats, edgeRID);

    var broken = false;

    if (edgeRID == null)
    // RID NULL
    {
      broken = true;
    } else {
      EntityImpl record = null;
      try {
        record = edgeRID.getIdentity().getRecord(session);
      } catch (RecordNotFoundException e) {
        broken = true;
      }

      if (record == null)
      // RECORD DELETED
      {
        broken = true;
      } else {
        SchemaImmutableClass immutableClass = null;
        if (record != null) {
          immutableClass = record.getImmutableSchemaClass(session);
        }
        if (immutableClass == null
            || (!immutableClass.isVertexType(session) && !immutableClass.isEdgeType(session)))
        // INVALID RECORD TYPE: NULL OR NOT GRAPH TYPE
        {
          broken = true;
        } else {
          if (immutableClass.isVertexType(session)) {
            // VERTEX -> LIGHTWEIGHT EDGE
            final var inverseFieldName =
                getInverseConnectionFieldName(fieldName, useVertexFieldsForEdgeLabels);

            // CHECK THE VERTEX IS IN INVERSE EDGE CONTAINS
            final var inverseEdgeContainer = record.field(inverseFieldName);
            if (inverseEdgeContainer == null)
            // NULL CONTAINER
            {
              broken = true;
            } else {

              if (inverseEdgeContainer instanceof Identifiable) {
                if (!inverseEdgeContainer.equals(vertex))
                // NOT THE SAME
                {
                  broken = true;
                }
              } else if (inverseEdgeContainer instanceof Collection<?>) {
                if (!((Collection) inverseEdgeContainer).contains(vertex))
                // NOT IN COLLECTION
                {
                  broken = true;
                }

              } else if (inverseEdgeContainer instanceof RidBag) {
                if (!((RidBag) inverseEdgeContainer).contains(vertex.getIdentity()))
                // NOT IN RIDBAG
                {
                  broken = true;
                }
              }
            }
          } else {
            // EDGE -> REGULAR EDGE, OK
            if (record.isStatefulEdge()) {
              var edge = record.castToStatefulEdge();
              final Identifiable backRID = edge.getVertex(direction);
              if (backRID == null || !backRID.equals(vertex))
              // BACK RID POINTS TO ANOTHER VERTEX
              {
                broken = true;
              }
            }
          }
        }
      }
    }

    if (broken) {
      onRemovedLink(stats, edgeRID);
      return true;
    }

    return false;
  }

  public static String getInverseConnectionFieldName(
      final String iFieldName, final boolean useVertexFieldsForEdgeLabels) {
    if (useVertexFieldsForEdgeLabels) {
      if (iFieldName.startsWith("out_")) {
        if (iFieldName.length() == "out_".length())
        // "OUT" CASE
        {
          return "in_";
        }

        return "in_" + iFieldName.substring("out_".length());

      } else if (iFieldName.startsWith("in_")) {
        if (iFieldName.length() == "in_".length())
        // "IN" CASE
        {
          return "out_";
        }

        return "out_" + iFieldName.substring("in_".length());

      } else {
        throw new IllegalArgumentException(
            "Cannot find reverse connection name for field " + iFieldName);
      }
    }

    if (iFieldName.equals("out")) {
      return "in";
    } else if (iFieldName.equals("in")) {
      return "out";
    }

    throw new IllegalArgumentException(
        "Cannot find reverse connection name for field " + iFieldName);
  }
}
