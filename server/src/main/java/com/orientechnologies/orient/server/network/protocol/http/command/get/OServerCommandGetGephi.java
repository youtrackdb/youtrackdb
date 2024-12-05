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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTVertex;
import com.orientechnologies.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OServerCommandGetGephi extends OServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = {"GET|gephi/*"};

  public OServerCommandGetGephi() {
  }

  public OServerCommandGetGephi(final OServerCommandConfiguration iConfig) {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts =
        checkSyntax(
            iRequest.getUrl(),
            4,
            "Syntax error:"
                + " gephi/<database>/<language>/<query-text>[/<limit>][/<fetchPlan>].<br>Limit is"
                + " optional and is set to 20 by default. Set to 0 to have no limits.");

    final String language = urlParts[2];
    final String text = urlParts[3];
    final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;
    final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;

    iRequest.getData().commandInfo = "Gephi";
    iRequest.getData().commandDetail = text;

    var db = getProfiledDatabaseInstance(iRequest);

    try {

      final YTResultSet resultSet;
      if (language.equals("sql")) {
        resultSet = executeStatement(language, text, new HashMap<>(), db);
      } else if (language.equals("gremlin")) {
        // TODO use TP3

        // EMPTY
        resultSet = null;
        //        List<Object> result = new ArrayList<Object>();
        //        OGremlinHelper.execute(graph, text, null, null, result, null, null);
        //
        //        resultSet = new ArrayList<YTEntity>(result.size());
        //
        //        for (Object o : result) {
        //          ((ArrayList<YTEntity>) resultSet).add(db.getVertex(o));
        //        }
      } else {
        throw new IllegalArgumentException(
            "Language '" + language + "' is not supported. Use 'sql' or 'gremlin'");
      }

      sendRecordsContent(iRequest, iResponse, resultSet, fetchPlan, limit);

    } finally {
      if (db != null) {
        db.close();
      }
    }

    return false;
  }

  protected void sendRecordsContent(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      YTResultSet resultSet,
      String iFetchPlan,
      int limit)
      throws IOException {

    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer, OHttpResponse.JSON_FORMAT);
    json.setPrettyPrint(true);

    generateGraphDbOutput(resultSet, limit, json);

    iResponse.send(
        OHttpUtils.STATUS_OK_CODE,
        OHttpUtils.STATUS_OK_DESCRIPTION,
        OHttpUtils.CONTENT_JSON,
        buffer.toString(),
        null);
  }

  protected void generateGraphDbOutput(
      final YTResultSet resultSet, int limit, final OJSONWriter json) throws IOException {
    if (resultSet == null) {
      return;
    }

    // CREATE A SET TO SPEED UP SEARCHES ON VERTICES
    final Set<YTVertex> vertexes = new HashSet<>();
    final Set<YTEdge> edges = new HashSet<>();

    int i = 0;
    while (resultSet.hasNext()) {
      if (limit >= 0 && i >= limit) {
        break;
      }
      YTResult next = resultSet.next();
      if (next.isVertex()) {
        vertexes.add(next.getVertex().get());
      } else if (next.isEdge()) {
        edges.add(next.getEdge().get());
      }
      i++;
    }
    resultSet.close();

    for (YTVertex vertex : vertexes) {
      json.resetAttributes();
      json.beginObject(0, false, null);
      json.beginObject(1, false, "an");
      json.beginObject(2, false, vertex.getIdentity());

      // ADD ALL THE EDGES
      vertex.getEdges(ODirection.BOTH).forEach(edges::add);

      // ADD ALL THE PROPERTIES
      for (String field : vertex.getPropertyNames()) {
        final Object v = vertex.getProperty(field);
        if (v != null) {
          json.writeAttribute(3, false, field, v);
        }
      }
      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }

    for (YTEdge edge : edges) {

      json.resetAttributes();
      json.beginObject();
      json.beginObject(1, false, "ae");
      json.beginObject(2, false, edge.getIdentity());
      json.writeAttribute(3, false, "directed", false);

      json.writeAttribute(3, false, "source", edge.getToIdentifiable());
      json.writeAttribute(3, false, "target", edge.getFromIdentifiable());

      for (String field : edge.getPropertyNames()) {
        final Object v = edge.getProperty(field);

        if (v != null) {
          json.writeAttribute(3, false, field, v);
        }
      }

      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  protected YTResultSet executeStatement(
      String language, String text, Object params, YTDatabaseSession db) {
    YTResultSet result;
    if (params instanceof Map) {
      result = db.command(text, (Map) params);
    } else if (params instanceof Object[]) {
      result = db.command(text, (Object[]) params);
    } else {
      result = db.command(text, params);
    }
    return result;
  }
}
