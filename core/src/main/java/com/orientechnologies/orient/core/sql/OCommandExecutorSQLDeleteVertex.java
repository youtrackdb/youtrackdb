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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL DELETE VERTEX command.
 */
public class OCommandExecutorSQLDeleteVertex extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest, OCommandResultListener {

  public static final String NAME = "DELETE VERTEX";
  private static final String KEYWORD_BATCH = "BATCH";
  private YTRecordId rid;
  private int removed = 0;
  private YTDatabaseSessionInternal database;
  private OCommandRequest query;
  private String returning = "COUNT";
  private List<YTRecord> allDeletedRecords;
  private final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
  private boolean txAlreadyBegun;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteVertex parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      database = getDatabase();

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword("DELETE");
      parserRequiredKeyword("VERTEX");

      YTClass clazz = null;
      String where = null;

      int limit = -1;
      String word = parseOptionalWord(true);
      while (word != null) {

        if (word.startsWith("#")) {
          rid = new YTRecordId(word);

        } else if (word.equalsIgnoreCase("from")) {
          final StringBuilder q = new StringBuilder();
          final int newPos =
              OStringSerializerHelper.getEmbedded(parserText, parserGetCurrentPosition(), -1, q);

          query = database.command(new OSQLAsynchQuery<YTDocument>(q.toString(), this));

          parserSetCurrentPosition(newPos);

        } else if (word.equals(KEYWORD_WHERE)) {
          if (clazz == null)
          // ASSIGN DEFAULT CLASS
          {
            clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass("V");
          }

          where =
              parserGetCurrentPosition() > -1
                  ? " " + parserText.substring(parserGetPreviousPosition())
                  : "";
          query =
              database.command(
                  new OSQLAsynchQuery<YTDocument>(
                      "select from `" + clazz.getName() + "`" + where, this));
          break;

        } else if (word.equals(KEYWORD_LIMIT)) {
          word = parseOptionalWord(true);
          try {
            limit = Integer.parseInt(word);
          } catch (Exception e) {
            throw OException.wrapException(
                new OCommandSQLParsingException("Invalid LIMIT: " + word), e);
          }
        } else if (word.equals(KEYWORD_RETURN)) {
          returning = parseReturn();

        } else if (word.equals(KEYWORD_BATCH)) {
          word = parserNextWord(true);
          if (word != null) {
            batch = Integer.parseInt(word);
          }

        } else if (word.length() > 0) {
          // GET/CHECK CLASS NAME
          clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(word);
          if (clazz == null) {
            throw new OCommandSQLParsingException("Class '" + word + "' was not found");
          }
        }

        word = parseOptionalWord(true);
        if (parserIsEnded()) {
          break;
        }
      }

      if (where == null) {
        where = "";
      } else {
        where = " WHERE " + where;
      }

      if (query == null && rid == null) {
        StringBuilder queryString = new StringBuilder();
        queryString.append("select from `");
        if (clazz == null) {
          queryString.append("V");
        } else {
          queryString.append(clazz.getName());
        }
        queryString.append("`");

        queryString.append(where);
        if (limit > -1) {
          queryString.append(" LIMIT ").append(limit);
        }
        query = database.command(new OSQLAsynchQuery<YTDocument>(queryString.toString(), this));
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the command and return the YTDocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (rid == null && query == null) {
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    if (!returning.equalsIgnoreCase("COUNT")) {
      allDeletedRecords = new ArrayList<YTRecord>();
    }

    txAlreadyBegun = getDatabase().getTransaction().isActive();

    YTDatabaseSessionInternal db = getDatabase();
    if (rid != null) {
      // REMOVE PUNCTUAL RID
      db.begin();
      final YTVertex v = toVertex(rid);
      if (v != null) {
        v.delete();
        removed = 1;
      }
      db.commit();

    } else if (query != null) {
      // TARGET IS A CLASS + OPTIONAL CONDITION
      db.begin();
      // TARGET IS A CLASS + OPTIONAL CONDITION

      query.setContext(getContext());
      query.execute(db, iArgs);
      db.commit();

    } else {
      throw new OCommandExecutionException("Invalid target");
    }

    if (returning.equalsIgnoreCase("COUNT"))
    // RETURNS ONLY THE COUNT
    {
      return removed;
    } else
    // RETURNS ALL THE DELETED RECORDS
    {
      return allDeletedRecords;
    }
  }

  /**
   * Delete the current vertex.
   */
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    final YTIdentifiable id = (YTIdentifiable) iRecord;
    if (id.getIdentity().isValid()) {
      final YTDocument record = id.getRecord();
      YTDatabaseSessionInternal db = getDatabase();

      final YTVertex v = toVertex(record);
      if (v != null) {
        v.delete();

        if (!txAlreadyBegun && batch > 0 && removed % batch == 0) {
          db.commit();
          db.begin();
        }

        if (returning.equalsIgnoreCase("BEFORE")) {
          allDeletedRecords.add(record);
        }

        removed++;
      }
    }

    return true;
  }

  @Override
  public long getDistributedTimeout() {
    return YTGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "DELETE VERTEX <rid>|<class>|FROM <query> [WHERE <conditions>] [LIMIT <max-records>]"
        + " [RETURN <COUNT|BEFORE>]> [BATCH <batch-size>]";
  }

  @Override
  public void end() {
    YTDatabaseSessionInternal db = getDatabase();
    if (!txAlreadyBegun) {
      db.commit();
    }
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected String parseReturn() throws OCommandSQLParsingException {
    final String returning = parserNextWord(true);

    if (!returning.equalsIgnoreCase("COUNT") && !returning.equalsIgnoreCase("BEFORE")) {
      throwParsingException(
          "Invalid "
              + KEYWORD_RETURN
              + " value set to '"
              + returning
              + "' but it should be COUNT (default), BEFORE. Example: "
              + KEYWORD_RETURN
              + " BEFORE");
    }

    return returning;
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
  public Set<String> getInvolvedClusters() {
    final HashSet<String> result = new HashSet<String>();
    if (rid != null) {
      result.add(database.getClusterNameById(rid.getClusterId()));
    } else if (query != null) {
      final OCommandExecutor executor =
          database
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

  /**
   * setLimit() for DELETE VERTEX is ignored. Please use LIMIT keyword in the SQL statement
   */
  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    // do nothing
    return (RET) this;
  }

  private static YTVertex toVertex(YTIdentifiable item) {
    if (item instanceof YTEntity) {
      return ((YTEntity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (ORecordNotFoundException rnf) {
        return null;
      }

      if (item instanceof YTEntity) {
        return ((YTEntity) item).asVertex().orElse(null);
      }
    }
    return null;
  }
}
