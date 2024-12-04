package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTVertexInternal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageRecoverEventListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Repairs a graph. Current implementation scan the entire graph. In the future the WAL will be used
 * to make this repair task much faster.
 */
public class OGraphRepair {

  private class ORepairStats {

    private long scannedEdges = 0;
    private long removedEdges = 0;
    private long scannedVertices = 0;
    private long scannedLinks = 0;
    private long removedLinks = 0;
    private long repairedVertices = 0;
  }

  private OStorageRecoverEventListener eventListener;

  public void repair(
      final YTDatabaseSession graph,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(outputListener, "Repair of graph '" + graph.getURL() + "' is started ...\n");

    final long beginTime = System.currentTimeMillis();

    final ORepairStats stats = new ORepairStats();

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
      final YTDatabaseSession graph,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(outputListener, "Check of graph '" + graph.getURL() + "' is started...\n");

    final long beginTime = System.currentTimeMillis();

    final ORepairStats stats = new ORepairStats();

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
      final YTDatabaseSession graph,
      final ORepairStats stats,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) graph;
    db.executeInTx(
        () -> {
          final OMetadata metadata = db.getMetadata();
          final YTSchema schema = metadata.getSchema();
          //    final OrientConfigurableGraph.Settings settings = graph.settings;

          final boolean useVertexFieldsForEdgeLabels = true; // db.isUseVertexFieldsForEdgeLabels();

          final YTClass edgeClass = schema.getClass(YTClass.EDGE_CLASS_NAME);
          if (edgeClass != null) {
            final long countEdges = db.countClass(edgeClass.getName());

            long skipEdges = 0L;
            if (options != null && options.get("-skipEdges") != null) {
              skipEdges = Long.parseLong(options.get("-skipEdges").get(0));
            }

            message(
                outputListener,
                "Scanning " + countEdges + " edges (skipEdges=" + skipEdges + ")...\n");

            long parsedEdges = 0L;
            final long beginTime = System.currentTimeMillis();

            for (YTDocument edge : db.browseClass(edgeClass.getName())) {
              if (!edge.isEdge()) {
                continue;
              }
              final YTRID edgeId = edge.getIdentity();

              parsedEdges++;
              if (skipEdges > 0 && parsedEdges <= skipEdges) {
                continue;
              }

              stats.scannedEdges++;

              if (eventListener != null) {
                eventListener.onScannedEdge(edge);
              }

              if (outputListener != null && stats.scannedEdges % 100000 == 0) {
                long speedPerSecond =
                    (long) (parsedEdges / ((System.currentTimeMillis() - beginTime) / 1000.0));
                if (speedPerSecond < 1) {
                  speedPerSecond = 1;
                }
                final long remaining = (countEdges - parsedEdges) / speedPerSecond;

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

              boolean outVertexMissing = false;

              String removalReason = "";

              final YTIdentifiable out = edge.asEdge().get().getFrom();
              if (out == null) {
                outVertexMissing = true;
              } else {
                YTDocument outVertex;
                try {
                  outVertex = out.getRecord();
                } catch (YTRecordNotFoundException e) {
                  outVertex = null;
                }

                if (outVertex == null) {
                  outVertexMissing = true;
                } else {
                  final String outFieldName =
                      YTVertexInternal.getEdgeLinkFieldName(
                          ODirection.OUT, edge.getClassName(), useVertexFieldsForEdgeLabels);

                  final Object outEdges = outVertex.field(outFieldName);
                  if (outEdges == null) {
                    outVertexMissing = true;
                  } else if (outEdges instanceof ORidBag) {
                    if (!((ORidBag) outEdges).contains(edgeId)) {
                      outVertexMissing = true;
                    }
                  } else if (outEdges instanceof Collection) {
                    if (!((Collection) outEdges).contains(edgeId)) {
                      outVertexMissing = true;
                    }
                  } else if (outEdges instanceof YTIdentifiable) {
                    if (((YTIdentifiable) outEdges).getIdentity().equals(edgeId)) {
                      outVertexMissing = true;
                    }
                  }
                }
              }

              if (outVertexMissing) {
                removalReason = "outgoing vertex (" + out + ") does not contain the edge";
              }

              boolean inVertexMissing = false;

              final YTIdentifiable in = edge.asEdge().get().getTo();
              if (in == null) {
                inVertexMissing = true;
              } else {

                YTDocument inVertex;
                try {
                  inVertex = in.getRecord();
                } catch (YTRecordNotFoundException e) {
                  inVertex = null;
                }

                if (inVertex == null) {
                  inVertexMissing = true;
                } else {
                  final String inFieldName =
                      YTVertexInternal.getEdgeLinkFieldName(
                          ODirection.IN, edge.getClassName(), useVertexFieldsForEdgeLabels);

                  final Object inEdges = inVertex.field(inFieldName);
                  if (inEdges == null) {
                    inVertexMissing = true;
                  } else if (inEdges instanceof ORidBag) {
                    if (!((ORidBag) inEdges).contains(edgeId)) {
                      inVertexMissing = true;
                    }
                  } else if (inEdges instanceof Collection) {
                    if (!((Collection) inEdges).contains(edgeId)) {
                      inVertexMissing = true;
                    }
                  } else if (inEdges instanceof YTIdentifiable) {
                    if (((YTIdentifiable) inEdges).getIdentity().equals(edgeId)) {
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
      final YTDatabaseSession graph,
      final ORepairStats stats,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) graph;
    final OMetadata metadata = db.getMetadata();
    final YTSchema schema = metadata.getSchema();

    final YTClass vertexClass = schema.getClass(YTClass.VERTEX_CLASS_NAME);
    if (vertexClass != null) {
      final long countVertices = db.countClass(vertexClass.getName());
      graph.executeInTx(
          () -> {
            long skipVertices = 0L;
            if (options != null && options.get("-skipVertices") != null) {
              skipVertices = Long.parseLong(options.get("-skipVertices").get(0));
            }

            message(outputListener, "Scanning " + countVertices + " vertices...\n");

            long[] parsedVertices = new long[]{0L};
            final long beginTime = System.currentTimeMillis();

            for (YTDocument vertex : db.browseClass(vertexClass.getName())) {
              parsedVertices[0]++;
              if (skipVertices > 0 && parsedVertices[0] <= skipVertices) {
                continue;
              }

              boolean vertexCorrupted = false;
              stats.scannedVertices++;
              if (eventListener != null) {
                eventListener.onScannedVertex(vertex);
              }

              if (outputListener != null && stats.scannedVertices % 100000 == 0) {
                long speedPerSecond =
                    (long)
                        (parsedVertices[0] / ((System.currentTimeMillis() - beginTime) / 1000.0));
                if (speedPerSecond < 1) {
                  speedPerSecond = 1;
                }
                final long remaining = (countVertices - parsedVertices[0]) / speedPerSecond;

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

              final YTVertex v = vertex.asVertex().orElse(null);
              if (v == null) {
                return;
              }

              for (String fieldName : vertex.fieldNames()) {
                final OPair<ODirection, String> connection =
                    YTVertexInternal.getConnection(
                        db.getMetadata().getSchema(), ODirection.BOTH, fieldName);
                if (connection == null) {
                  continue;
                }

                final Object fieldValue = vertex.rawField(fieldName);
                if (fieldValue != null) {
                  if (fieldValue instanceof YTIdentifiable) {

                    if (isEdgeBroken(
                        vertex,
                        fieldName,
                        connection.getKey(),
                        (YTIdentifiable) fieldValue,
                        stats,
                        true)) {
                      vertexCorrupted = true;
                      if (!checkOnly) {
                        vertex.field(fieldName, (Object) null);
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

                    for (Iterator<?> it = coll.iterator(); it.hasNext(); ) {
                      final Object o = it.next();

                      if (isEdgeBroken(
                          vertex, fieldName, connection.getKey(), (YTIdentifiable) o, stats,
                          true)) {
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

                  } else if (fieldValue instanceof ORidBag ridbag) {
                    // In case of ridbags force save for trigger eventual conversions
                    if (ridbag.size() == 0) {
                      vertex.removeField(fieldName);
                    } else if (!ridbag.isEmbedded()
                        && ridbag.size()
                        < YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD
                        .getValueAsInteger()) {
                      vertex.setDirty();
                    }
                    for (Iterator<?> it = ridbag.iterator(); it.hasNext(); ) {
                      final Object o = it.next();
                      if (isEdgeBroken(
                          vertex, fieldName, connection.getKey(), (YTIdentifiable) o, stats,
                          true)) {
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
                  vertex.save();
                }
              } else if (vertex.isDirty() && !checkOnly) {
                message(outputListener, "+ optimized vertex " + vertex + "\n");
                vertex.save();
              }
            }

            message(outputListener, "Scanning vertices completed\n");
          });
    }
  }

  private void onScannedLink(final ORepairStats stats, final YTIdentifiable fieldValue) {
    stats.scannedLinks++;
    if (eventListener != null) {
      eventListener.onScannedLink(fieldValue);
    }
  }

  private void onRemovedLink(final ORepairStats stats, final YTIdentifiable fieldValue) {
    stats.removedLinks++;
    if (eventListener != null) {
      eventListener.onRemovedLink(fieldValue);
    }
  }

  public OStorageRecoverEventListener getEventListener() {
    return eventListener;
  }

  public OGraphRepair setEventListener(final OStorageRecoverEventListener eventListener) {
    this.eventListener = eventListener;
    return this;
  }

  private void message(final OCommandOutputListener outputListener, final String message) {
    if (outputListener != null) {
      outputListener.onMessage(message);
    }
  }

  private boolean isEdgeBroken(
      final YTIdentifiable vertex,
      final String fieldName,
      final ODirection direction,
      final YTIdentifiable edgeRID,
      final ORepairStats stats,
      final boolean useVertexFieldsForEdgeLabels) {
    onScannedLink(stats, edgeRID);

    boolean broken = false;

    if (edgeRID == null)
    // RID NULL
    {
      broken = true;
    } else {
      YTDocument record = null;
      try {
        record = edgeRID.getIdentity().getRecord();
      } catch (YTRecordNotFoundException e) {
        broken = true;
      }

      if (record == null)
      // RECORD DELETED
      {
        broken = true;
      } else {
        final YTImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(record);
        if (immutableClass == null
            || (!immutableClass.isVertexType() && !immutableClass.isEdgeType()))
        // INVALID RECORD TYPE: NULL OR NOT GRAPH TYPE
        {
          broken = true;
        } else {
          if (immutableClass.isVertexType()) {
            // VERTEX -> LIGHTWEIGHT EDGE
            final String inverseFieldName =
                getInverseConnectionFieldName(fieldName, useVertexFieldsForEdgeLabels);

            // CHECK THE VERTEX IS IN INVERSE EDGE CONTAINS
            final Object inverseEdgeContainer = record.field(inverseFieldName);
            if (inverseEdgeContainer == null)
            // NULL CONTAINER
            {
              broken = true;
            } else {

              if (inverseEdgeContainer instanceof YTIdentifiable) {
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

              } else if (inverseEdgeContainer instanceof ORidBag) {
                if (!((ORidBag) inverseEdgeContainer).contains(vertex))
                // NOT IN RIDBAG
                {
                  broken = true;
                }
              }
            }
          } else {
            // EDGE -> REGULAR EDGE, OK
            YTEdge edge = record.asEdge().orElse(null);
            if (edge != null) {
              final YTIdentifiable backRID = edge.getVertex(direction);
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
