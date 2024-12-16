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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.types.ModifiableBoolean;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandExecutor;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestInternal;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLAsynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL DELETE VERTEX command.
 */
public class CommandExecutorSQLDeleteVertex extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest, CommandResultListener {

  public static final String NAME = "DELETE VERTEX";
  private static final String KEYWORD_BATCH = "BATCH";
  private RecordId rid;
  private int removed = 0;
  private DatabaseSessionInternal database;
  private CommandRequest query;
  private String returning = "COUNT";
  private List<Record> allDeletedRecords;
  private final ModifiableBoolean shutdownFlag = new ModifiableBoolean();
  private boolean txAlreadyBegun;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLDeleteVertex parse(final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      database = getDatabase();

      init((CommandRequestText) iRequest);

      parserRequiredKeyword("DELETE");
      parserRequiredKeyword("VERTEX");

      SchemaClass clazz = null;
      String where = null;

      int limit = -1;
      String word = parseOptionalWord(true);
      while (word != null) {

        if (word.startsWith("#")) {
          rid = new RecordId(word);

        } else if (word.equalsIgnoreCase("from")) {
          final StringBuilder q = new StringBuilder();
          final int newPos =
              StringSerializerHelper.getEmbedded(parserText, parserGetCurrentPosition(), -1, q);

          query = database.command(new SQLAsynchQuery<EntityImpl>(q.toString(), this));

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
                  new SQLAsynchQuery<EntityImpl>(
                      "select from `" + clazz.getName() + "`" + where, this));
          break;

        } else if (word.equals(KEYWORD_LIMIT)) {
          word = parseOptionalWord(true);
          try {
            limit = Integer.parseInt(word);
          } catch (Exception e) {
            throw BaseException.wrapException(
                new CommandSQLParsingException("Invalid LIMIT: " + word), e);
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
            throw new CommandSQLParsingException("Class '" + word + "' was not found");
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
        query = database.command(new SQLAsynchQuery<EntityImpl>(queryString.toString(), this));
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the command and return the EntityImpl object created.
   */
  public Object execute(final Map<Object, Object> iArgs, DatabaseSessionInternal querySession) {
    if (rid == null && query == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    if (!returning.equalsIgnoreCase("COUNT")) {
      allDeletedRecords = new ArrayList<Record>();
    }

    txAlreadyBegun = getDatabase().getTransaction().isActive();

    DatabaseSessionInternal db = getDatabase();
    if (rid != null) {
      // REMOVE PUNCTUAL RID
      db.begin();
      final Vertex v = toVertex(rid);
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
      throw new CommandExecutionException("Invalid target");
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
  public boolean result(DatabaseSessionInternal querySession, final Object iRecord) {
    final Identifiable id = (Identifiable) iRecord;
    if (((RecordId) id.getIdentity()).isValid()) {
      final EntityImpl record = id.getRecord();
      DatabaseSessionInternal db = getDatabase();

      final Vertex v = toVertex(record);
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
    return GlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "DELETE VERTEX <rid>|<class>|FROM <query> [WHERE <conditions>] [LIMIT <max-records>]"
        + " [RETURN <COUNT|BEFORE>]> [BATCH <batch-size>]";
  }

  @Override
  public void end() {
    DatabaseSessionInternal db = getDatabase();
    if (!txAlreadyBegun) {
      db.commit();
    }
  }

  @Override
  public int getSecurityOperationType() {
    return Role.PERMISSION_DELETE;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected String parseReturn() throws CommandSQLParsingException {
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
      final CommandExecutor executor =
          database
              .getSharedContext()
              .getYouTrackDB()
              .getScriptManager()
              .getCommandManager()
              .getExecutor((CommandRequestInternal) query);
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
  public <RET extends CommandExecutor> RET setLimit(final int iLimit) {
    // do nothing
    return (RET) this;
  }

  private static Vertex toVertex(Identifiable item) {
    if (item instanceof Entity) {
      return ((Entity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (RecordNotFoundException rnf) {
        return null;
      }

      if (item instanceof Entity) {
        return ((Entity) item).asVertex().orElse(null);
      }
    }
    return null;
  }
}
