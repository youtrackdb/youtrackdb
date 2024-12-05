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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL DELETE EDGE command.
 */
public class OCommandExecutorSQLDeleteEdge extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest, OCommandResultListener {

  public static final String NAME = "DELETE EDGE";
  private static final String KEYWORD_BATCH = "BATCH";
  private List<YTRecordId> rids;
  private String fromExpr;
  private String toExpr;
  private int removed = 0;
  private OCommandRequest query;
  private OSQLFilter compiledFilter;
  //  private AtomicReference<OrientBaseGraph> currentGraph  = new
  // AtomicReference<OrientBaseGraph>();
  private String label;
  private final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
  private boolean txAlreadyBegun;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteEdge parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;

    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword("DELETE");
      parserRequiredKeyword("EDGE");

      YTClass clazz = null;
      String where = null;

      String temp = parseOptionalWord(true);
      String originalTemp = null;

      int limit = -1;

      if (temp != null && !parserIsEnded()) {
        originalTemp =
            parserText.substring(parserGetPreviousPosition(), parserGetCurrentPosition()).trim();
      }

      YTDatabaseSessionInternal curDb = ODatabaseRecordThreadLocal.instance().get();
      //      final OrientGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false,
      // shutdownFlag);
      try {
        while (temp != null) {

          if (temp.equals("FROM")) {
            fromExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null) {
              throwSyntaxErrorException(
                  "FROM '" + fromExpr + "' is not allowed when specify a RIDs (" + rids + ")");
            }

          } else if (temp.equals("TO")) {
            toExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null) {
              throwSyntaxErrorException(
                  "TO '" + toExpr + "' is not allowed when specify a RID (" + rids + ")");
            }

          } else if (temp.startsWith("#")) {
            rids = new ArrayList<YTRecordId>();
            rids.add(new YTRecordId(temp));
            if (fromExpr != null || toExpr != null) {
              throwSyntaxErrorException(
                  "Specifying the RID " + rids + " is not allowed with FROM/TO");
            }

          } else if (temp.startsWith("[") && temp.endsWith("]")) {
            temp = temp.substring(1, temp.length() - 1);
            rids = new ArrayList<YTRecordId>();
            for (String rid : temp.split(",")) {
              rid = rid.trim();
              if (!rid.startsWith("#")) {
                throwSyntaxErrorException("Not a valid RID: " + rid);
              }
              rids.add(new YTRecordId(rid));
            }
          } else if (temp.equals(KEYWORD_WHERE)) {
            if (clazz == null)
            // ASSIGN DEFAULT CLASS
            {
              clazz = curDb.getMetadata().getImmutableSchemaSnapshot().getClass("E");
            }

            where =
                parserGetCurrentPosition() > -1
                    ? " " + parserText.substring(parserGetCurrentPosition())
                    : "";

            compiledFilter =
                OSQLEngine.parseCondition(where, getContext(), KEYWORD_WHERE);
            break;

          } else if (temp.equals(KEYWORD_BATCH)) {
            temp = parserNextWord(true);
            if (temp != null) {
              batch = Integer.parseInt(temp);
            }

          } else if (temp.equals(KEYWORD_LIMIT)) {
            temp = parserNextWord(true);
            if (temp != null) {
              limit = Integer.parseInt(temp);
            }

          } else if (temp.length() > 0) {
            // GET/CHECK CLASS NAME
            label = originalTemp;
            clazz = curDb.getMetadata().getSchema().getClass(temp);
            if (clazz == null) {
              throw new YTCommandSQLParsingException("Class '" + temp + "' was not found");
            }
          }

          temp = parseOptionalWord(true);
          if (parserIsEnded()) {
            break;
          }
        }

        if (where == null) {
          if (limit > -1) {
            where = " LIMIT " + limit;
          } else {
            where = "";
          }
        } else {
          where = " WHERE " + where;
        }

        if (fromExpr == null && toExpr == null && rids == null) {
          if (clazz == null)
          // DELETE ALL THE EDGES
          {
            query = curDb.command(new OSQLAsynchQuery<YTEntityImpl>("select from E" + where, this));
          } else
          // DELETE EDGES OF CLASS X
          {
            query =
                curDb.command(
                    new OSQLAsynchQuery<YTEntityImpl>(
                        "select from `" + clazz.getName() + "` " + where, this));
          }
        }

        return this;
      } finally {
        ODatabaseRecordThreadLocal.instance().set(curDb);
      }
    } finally {
      textRequest.setText(originalQuery);
    }
  }

  /**
   * Execute the command and return the YTEntityImpl object created.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (fromExpr == null
        && toExpr == null
        && rids == null
        && query == null
        && compiledFilter == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }
    YTDatabaseSessionInternal db = getDatabase();
    txAlreadyBegun = db.getTransaction().isActive();

    if (rids != null) {
      // REMOVE PUNCTUAL RID
      db.begin();
      for (YTRecordId rid : rids) {
        final YTEdge e = toEdge(rid);
        if (e != null) {
          e.delete();
          removed++;
        }
      }
      db.commit();
      return removed;
    } else {
      // MULTIPLE EDGES
      final Set<YTEdge> edges = new HashSet<YTEdge>();
      if (query == null) {
        db.begin();
        Set<YTIdentifiable> fromIds = null;
        if (fromExpr != null) {
          fromIds = OSQLEngine.getInstance().parseRIDTarget(db, fromExpr, context, iArgs);
        }
        Set<YTIdentifiable> toIds = null;
        if (toExpr != null) {
          toIds = OSQLEngine.getInstance().parseRIDTarget(db, toExpr, context, iArgs);
        }
        if (label == null) {
          label = "E";
        }

        if (fromIds != null && toIds != null) {
          int fromCount = 0;
          int toCount = 0;
          for (YTIdentifiable fromId : fromIds) {
            final YTVertex v = toVertex(fromId);
            if (v != null) {
              fromCount += count(v.getEdges(ODirection.OUT, label));
            }
          }
          for (YTIdentifiable toId : toIds) {
            final YTVertex v = toVertex(toId);
            if (v != null) {
              toCount += count(v.getEdges(ODirection.IN, label));
            }
          }
          if (fromCount <= toCount) {
            // REMOVE ALL THE EDGES BETWEEN VERTICES
            for (YTIdentifiable fromId : fromIds) {
              final YTVertex v = toVertex(fromId);
              if (v != null) {
                for (YTEdge e : v.getEdges(ODirection.OUT, label)) {
                  final YTIdentifiable inV = e.getTo();
                  if (inV != null && toIds.contains(inV.getIdentity())) {
                    edges.add(e);
                  }
                }
              }
            }
          } else {
            for (YTIdentifiable toId : toIds) {
              final YTVertex v = toVertex(toId);
              if (v != null) {
                for (YTEdge e : v.getEdges(ODirection.IN, label)) {
                  final YTRID outVRid = e.getFromIdentifiable().getIdentity();
                  if (outVRid != null && fromIds.contains(outVRid)) {
                    edges.add(e);
                  }
                }
              }
            }
          }
        } else if (fromIds != null) {
          // REMOVE ALL THE EDGES THAT START FROM A VERTEXES
          for (YTIdentifiable fromId : fromIds) {

            final YTVertex v = toVertex(fromId);
            if (v != null) {
              for (YTEdge e : v.getEdges(ODirection.OUT, label)) {
                edges.add(e);
              }
            }
          }
        } else if (toIds != null) {
          // REMOVE ALL THE EDGES THAT ARRIVE TO A VERTEXES
          for (YTIdentifiable toId : toIds) {
            final YTVertex v = toVertex(toId);
            if (v != null) {
              for (YTEdge e : v.getEdges(ODirection.IN, label)) {
                edges.add(e);
              }
            }
          }
        } else {
          throw new YTCommandExecutionException("Invalid target: " + toIds);
        }

        if (compiledFilter != null) {
          // ADDITIONAL FILTERING
          for (Iterator<YTEdge> it = edges.iterator(); it.hasNext(); ) {
            final YTEdge edge = it.next();
            if (!(Boolean) compiledFilter.evaluate(edge.getRecord(), null, context)) {
              it.remove();
            }
          }
        }

        // DELETE THE FOUND EDGES
        removed = edges.size();
        for (YTEdge edge : edges) {
          edge.delete();
        }

        db.commit();
        return removed;

      } else {
        db.begin();
        // TARGET IS A CLASS + OPTIONAL CONDITION
        query.setContext(getContext());
        query.execute(querySession, iArgs);
        db.commit();
        return removed;
      }
    }
  }

  private int count(Iterable<YTEdge> edges) {
    int result = 0;
    for (YTEdge x : edges) {
      result++;
    }
    return result;
  }

  /**
   * Delete the current edge.
   */
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    final YTIdentifiable id = (YTIdentifiable) iRecord;

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(id.getRecord(), null, context)) {
        return true;
      }
    }

    if (id.getIdentity().isValid()) {

      final YTEdge e = toEdge(id);

      if (e != null) {
        e.delete();

        if (!txAlreadyBegun && batch > 0 && (removed + 1) % batch == 0) {
          getDatabase().commit();
          getDatabase().begin();
        }

        removed++;
      }
    }

    return true;
  }

  private YTEdge toEdge(YTIdentifiable item) {
    if (item != null && item instanceof YTEntity) {
      final YTIdentifiable a = item;
      return ((YTEntity) item)
          .asEdge()
          .orElseThrow(
              () -> new YTCommandExecutionException((a.getIdentity()) + " is not an edge"));
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }

      if (item instanceof YTEntity) {
        final YTIdentifiable a = item;
        return ((YTEntity) item)
            .asEdge()
            .orElseThrow(
                () -> new YTCommandExecutionException((a.getIdentity()) + " is not an edge"));
      }
    }
    return null;
  }

  private YTVertex toVertex(YTIdentifiable item) {
    if (item instanceof YTEntity) {
      return ((YTEntity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }
      if (item instanceof YTEntity) {
        return ((YTEntity) item).asVertex().orElse(null);
      }
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "DELETE EDGE <rid>|FROM <rid>|TO <rid>|<[<class>] [WHERE <conditions>]> [BATCH"
        + " <batch-size>]";
  }

  @Override
  public void end() {
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    if (getDistributedExecutionMode() == DISTRIBUTED_EXECUTION_MODE.LOCAL) {
      return DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
    } else {
      return DISTRIBUTED_RESULT_MGMT.MERGE;
    }
  }

  @Override
  public Object getResult() {
    return null;
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    if (query != null && !getDatabase().getTransaction().isActive()) {
      return DISTRIBUTED_EXECUTION_MODE.REPLICATE;
    } else {
      return DISTRIBUTED_EXECUTION_MODE.LOCAL;
    }
  }

  @Override
  public Set<String> getInvolvedClusters() {
    final HashSet<String> result = new HashSet<String>();
    if (rids != null) {
      final YTDatabaseSessionInternal database = getDatabase();
      for (YTRecordId rid : rids) {
        result.add(database.getClusterNameById(rid.getClusterId()));
      }
    } else if (query != null) {

      final OCommandExecutor executor =
          getDatabase()
              .getSharedContext()
              .getYouTrackDB()
              .getScriptManager()
              .getCommandManager()
              .getExecutor((OCommandRequestInternal) query);
      // COPY THE CONTEXT FROM THE REQUEST
      executor.setContext(context);
      executor.parse(query);
      return executor.getInvolvedClusters();
    }
    return result;
  }

  /**
   * setLimit() for DELETE EDGE is ignored. Please use LIMIT keyword in the SQL statement
   */
  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    // do nothing
    return (RET) this;
  }
}
