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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItem;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionRuntime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL CREATE EDGE command.
 */
public class CommandExecutorSQLCreateEdge extends CommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest {

  public static final String NAME = "CREATE EDGE";
  private static final String KEYWORD_BATCH = "BATCH";

  private String from;
  private String to;
  private YTClass clazz;
  private String edgeLabel;
  private String clusterName;
  private List<OPair<String, Object>> fields;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLCreateEdge parse(final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final var database = getDatabase();

      init((CommandRequestText) iRequest);

      parserRequiredKeyword("CREATE");
      parserRequiredKeyword("EDGE");

      String className = null;

      String tempLower = parseOptionalWord(false);
      String temp = tempLower == null ? null : tempLower.toUpperCase(Locale.ENGLISH);

      while (temp != null) {
        if (temp.equals("CLUSTER")) {
          clusterName = parserRequiredWord(false);

        } else if (temp.equals(KEYWORD_FROM)) {
          from = parserRequiredWord(false, "Syntax error", " =><,\r\n");

        } else if (temp.equals("TO")) {
          to = parserRequiredWord(false, "Syntax error", " =><,\r\n");

        } else if (temp.equals(KEYWORD_SET)) {
          fields = new ArrayList<OPair<String, Object>>();
          parseSetFields(clazz, fields);

        } else if (temp.equals(KEYWORD_CONTENT)) {
          parseContent();

        } else if (temp.equals(KEYWORD_BATCH)) {
          temp = parserNextWord(true);
          if (temp != null) {
            batch = Integer.parseInt(temp);
          }

        } else if (className == null && !temp.isEmpty()) {
          className = tempLower;

          clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(temp);
          if (clazz == null) {
            final int committed;
            if (database.getTransaction().isActive()) {
              LogManager.instance()
                  .warn(
                      this,
                      "Requested command '"
                          + this
                          + "' must be executed outside active transaction: the transaction will be"
                          + " committed and reopen right after it. To avoid this behavior execute"
                          + " it outside a transaction");
              committed = database.getTransaction().amountOfNestedTxs();
              database.commit();
            } else {
              committed = 0;
            }

            try {
              YTSchema schema = database.getMetadata().getSchema();
              YTClass e = schema.getClass("E");
              clazz = schema.createClass(className, e);
            } finally {
              // RESTART TRANSACTION
              for (int i = 0; i < committed; ++i) {
                database.begin();
              }
            }
          }
        }

        temp = parseOptionalWord(true);
        if (parserIsEnded()) {
          break;
        }
      }

      if (className == null) {
        // ASSIGN DEFAULT CLASS
        className = "E";
        clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(className);
      }

      // GET/CHECK CLASS NAME
      if (clazz == null) {
        throw new YTCommandSQLParsingException("Class '" + className + "' was not found");
      }

      edgeLabel = className;
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  /**
   * Execute the command and return the EntityImpl object created.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (clazz == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    YTDatabaseSessionInternal db = getDatabase();
    final List<Object> edges = new ArrayList<Object>();
    Set<YTIdentifiable> fromIds = null;
    Set<YTIdentifiable> toIds = null;
    db.begin();
    try {
      fromIds = OSQLEngine.getInstance().parseRIDTarget(db, from, context, iArgs);
      toIds = OSQLEngine.getInstance().parseRIDTarget(db, to, context, iArgs);

      // CREATE EDGES
      for (YTIdentifiable from : fromIds) {
        final Vertex fromVertex = toVertex(from);
        if (fromVertex == null) {
          throw new YTCommandExecutionException("Source vertex '" + from + "' does not exist");
        }

        for (YTIdentifiable to : toIds) {
          final Vertex toVertex;
          if (from.equals(to)) {
            toVertex = fromVertex;
          } else {
            toVertex = toVertex(to);
          }
          if (toVertex == null) {
            throw new YTCommandExecutionException("Source vertex '" + to + "' does not exist");
          }

          if (fields != null)
          // EVALUATE FIELDS
          {
            for (final OPair<String, Object> f : fields) {
              if (f.getValue() instanceof OSQLFunctionRuntime) {
                f.setValue(((OSQLFunctionRuntime) f.getValue()).getValue(to, null, context));
              } else if (f.getValue() instanceof OSQLFilterItem) {
                f.setValue(((OSQLFilterItem) f.getValue()).getValue(to, null, context));
              }
            }
          }

          EdgeInternal edge;

          if (content != null) {
            if (fields != null)
            // MERGE CONTENT WITH FIELDS
            {
              fields.addAll(OPair.convertFromMap(content.toMap()));
            } else {
              fields = OPair.convertFromMap(content.toMap());
            }
          }

          edge = (EdgeInternal) fromVertex.addEdge(toVertex, edgeLabel);
          if (fields != null && !fields.isEmpty()) {
            OSQLHelper.bindParameters(
                edge.getRecord(), fields, new OCommandParameters(iArgs), context);
          }

          edge.getBaseDocument().save(clusterName);
          fromVertex.save();
          toVertex.save();

          edges.add(edge);

          if (batch > 0 && edges.size() % batch == 0) {
            db.commit();
            db.begin();
          }
        }
      }

    } finally {
      db.commit();
    }

    if (edges.isEmpty()) {
      if (fromIds.isEmpty()) {
        throw new YTCommandExecutionException(
            "No edge has been created because no source vertices: " + this);
      } else if (toIds.isEmpty()) {
        throw new YTCommandExecutionException(
            "No edge has been created because no target vertices: " + this);
      }
      throw new YTCommandExecutionException(
          "No edge has been created between " + fromIds + " and " + toIds + ": " + this);
    }
    return edges;
  }

  private static Vertex toVertex(YTIdentifiable item) {
    if (item == null) {
      return null;
    }
    if (item instanceof Entity) {
      return ((Entity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (YTRecordNotFoundException e) {
        return null;
      }
      if (item instanceof Entity) {
        return ((Entity) item).asVertex().orElse(null);
      }
    }
    return null;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public Set getInvolvedClusters() {
    if (clazz != null) {
      return Collections.singleton(
          getDatabase().getClusterNameById(clazz.getClusterSelection().getCluster(clazz, null)));
    } else if (clusterName != null) {
      return getInvolvedClustersOfClusters(Collections.singleton(clusterName));
    }

    return Collections.EMPTY_SET;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] "
        + "FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) "
        + "[SET <field> = <expression>[,]*]|CONTENT {<JSON>} "
        + "[RETRY <retry> [WAIT <pauseBetweenRetriesInMs]] [BATCH <batch-size>]";
  }
}
