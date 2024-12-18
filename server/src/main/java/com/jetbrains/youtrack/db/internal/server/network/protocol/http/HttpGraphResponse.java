/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.server.network.protocol.http;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.VertexInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.JSONWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Graph wrapper to format the response as graph.
 */
public class HttpGraphResponse extends HttpResponseAbstract {

  private final HttpResponse iWrapped;

  public HttpGraphResponse(final HttpResponse iWrapped) {
    super(
        iWrapped.getOutputStream(),
        iWrapped.getHttpVersion(),
        iWrapped.getAdditionalHeaders(),
        iWrapped.getCharacterSet(),
        iWrapped.getServerInfo(),
        iWrapped.getSessionId(),
        iWrapped.getCallbackFunction(),
        iWrapped.isKeepAlive(),
        iWrapped.getConnection(),
        iWrapped.getContextConfiguration());
    this.iWrapped = iWrapped;
  }

  public void writeRecords(
      final Object iRecords,
      final String iFetchPlan,
      String iFormat,
      final String accept,
      final Map<String, Object> iAdditionalProperties,
      final String mode,
      DatabaseSessionInternal db)
      throws IOException {
    if (iRecords == null) {
      return;
    }

    if (!mode.equalsIgnoreCase("graph")) {
      super.writeRecords(
          iRecords,
          iFetchPlan,
          iFormat,
          accept,
          iAdditionalProperties,
          mode,
          db);
      return;
    }

    if (accept != null && accept.contains("text/csv")) {
      throw new IllegalArgumentException("Graph mode cannot accept '" + accept + "'");
    }

    DatabaseSessionInternal graph = DatabaseRecordThreadLocal.instance().get();

    try {
      // DIVIDE VERTICES FROM EDGES
      final Set<Vertex> vertices = new HashSet<>();

      Set<RID> edgeRids = new HashSet<RID>();
      boolean lightweightFound = false;

      final Iterator<?> iIterator = MultiValue.getMultiValueIterator(iRecords);
      while (iIterator.hasNext()) {
        Object entry = iIterator.next();

        if (entry != null && entry instanceof Result && ((Result) entry).isEntity()) {

          entry = ((Result) entry).getEntity().get();

        } else if (entry == null || !(entry instanceof Identifiable)) {
          // IGNORE IT
          continue;
        }

        try {
          entry = ((Identifiable) entry).getRecord(graph);
        } catch (Exception e) {
          // IGNORE IT
          continue;
        }
        entry = ((Identifiable) entry).getRecord(graph);

        if (entry instanceof Entity element) {
          if (element.isVertex()) {
            vertices.add(element.asVertex().get());
          } else if (element.isEdge()) {
            Edge edge = element.asEdge().get();
            vertices.add(edge.getTo());
            vertices.add(edge.getFrom());
            if (edge.getIdentity() != null) {
              edgeRids.add(edge.getIdentity());
            } else {
              lightweightFound = true;
            }
          } else
          // IGNORE IT
          {
            continue;
          }
        }
      }

      final StringWriter buffer = new StringWriter();
      final JSONWriter json = new JSONWriter(buffer, "");
      json.beginObject();
      json.beginObject("graph");

      // WRITE VERTICES
      json.beginCollection(graph, "vertices");
      for (Vertex vertex : vertices) {
        json.beginObject();

        json.writeAttribute(graph, "@rid", vertex.getIdentity());
        json.writeAttribute(graph, "@class", vertex.getSchemaType().get().getName());

        // ADD ALL THE PROPERTIES
        for (String field : ((VertexInternal) vertex).getPropertyNamesInternal()) {
          final Object v = ((VertexInternal) vertex).getPropertyInternal(field);
          if (v != null) {
            json.writeAttribute(graph, field, v);
          }
        }
        json.endObject();
      }
      json.endCollection();

      if (lightweightFound) {
        // clean up cached edges and re-calculate, there could be more
        edgeRids.clear();
      }

      // WRITE EDGES
      json.beginCollection(graph, "edges");

      if (edgeRids.isEmpty()) {
        for (Vertex vertex : vertices) {
          for (Edge e : vertex.getEdges(Direction.OUT)) {
            Edge edge = e;
            if (edgeRids.contains(e.getIdentity())
                && e.getIdentity() != null /* only for non-lighweight */) {
              continue;
            }
            if (!vertices.contains(edge.getVertex(Direction.OUT))
                || !vertices.contains(edge.getVertex(Direction.IN)))
            // ONE OF THE 2 VERTICES ARE NOT PART OF THE RESULT SET: DISCARD IT
            {
              continue;
            }

            edgeRids.add(edge.getIdentity());

            printEdge(graph, json, edge);
          }
        }
      } else {
        for (RID edgeRid : edgeRids) {
          try {
            Entity elem = edgeRid.getRecord(graph);
            Edge edge = elem.asEdge().orElse(null);

            if (edge != null) {
              printEdge(graph, json, edge);
            }
          } catch (RecordNotFoundException rnf) {
            // ignore
          }
        }
      }

      json.endCollection();

      if (iAdditionalProperties != null) {
        for (Map.Entry<String, Object> entry : iAdditionalProperties.entrySet()) {

          final Object v = entry.getValue();
          if (MultiValue.isMultiValue(v)) {
            json.beginCollection(graph, -1, true, entry.getKey());
            formatMultiValue(MultiValue.getMultiValueIterator(v), buffer, null, graph);
            json.endCollection(-1, true);
          } else {
            json.writeAttribute(graph, entry.getKey(), v);
          }

          if (Thread.currentThread().isInterrupted()) {
            break;
          }
        }
      }

      json.endObject();
      json.endObject();

      send(
          HttpUtils.STATUS_OK_CODE,
          HttpUtils.STATUS_OK_DESCRIPTION,
          HttpUtils.CONTENT_JSON,
          buffer.toString(),
          null);
    } finally {
      graph.close();
    }
  }

  private static void printEdge(DatabaseSessionInternal db, JSONWriter json, Edge edge)
      throws IOException {
    json.beginObject();
    json.writeAttribute(db, "@rid", edge.getIdentity());
    json.writeAttribute(db, "@class", edge.getSchemaType().map(x -> x.getName()).orElse(null));

    json.writeAttribute(db, "out", edge.getVertex(Direction.OUT).getIdentity());
    json.writeAttribute(db, "in", edge.getVertex(Direction.IN).getIdentity());

    for (String field : edge.getPropertyNames()) {
      final Object v = edge.getProperty(field);
      if (v != null) {
        json.writeAttribute(db, field, v);
      }
    }

    json.endObject();
  }

  @Override
  public void send(
      final int iCode,
      final String iReason,
      final String iContentType,
      final Object iContent,
      final String iHeaders)
      throws IOException {
    iWrapped.send(iCode, iReason, iContentType, iContent, iHeaders);
  }

  @Override
  public void writeStatus(final int iStatus, final String iReason) throws IOException {
    writeLine(getHttpVersion() + " " + iStatus + " " + iReason);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, null, null);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize,
      final String iFileName)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, iFileName, null);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize,
      final String iFileName,
      Map<String, String> additionalHeaders)
      throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    if (additionalHeaders != null) {
      for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
        writeLine(String.format("%s: %s", entry.getKey(), entry.getValue()));
      }
    }
    if (iSize < 0) {
      // SIZE UNKNOWN: USE A MEMORY BUFFER
      final ByteArrayOutputStream o = new ByteArrayOutputStream();
      if (iContent != null) {
        int b;
        while ((b = iContent.read()) > -1) {
          o.write(b);
        }
      }

      byte[] content = o.toByteArray();

      iContent = new ByteArrayInputStream(content);
      iSize = content.length;
    }

    writeLine(HttpUtils.HEADER_CONTENT_LENGTH + (iSize));
    writeLine(null);

    if (iContent != null) {
      int b;
      while ((b = iContent.read()) > -1) {
        getOut().write(b);
      }
    }

    flush();
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      final String iFileName,
      final CallableFunction<Void, ChunkedResponse> iWriter)
      throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");
    writeLine("Transfer-Encoding: chunked");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    writeLine(null);

    final ChunkedResponse chunkedOutput = new ChunkedResponse(this);
    iWriter.call(chunkedOutput);
    chunkedOutput.close();

    flush();
  }

  @Override
  public void checkConnection() throws IOException {
    iWrapped.checkConnection();
  }
}
