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

import com.jetbrains.youtrack.db.internal.common.parser.OStringParser;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OCompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OCompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.OIndexAbstract;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OIndexInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLDeleteStatement;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLQuery;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SQL UPDATE command.
 */
public class CommandExecutorSQLDelete extends CommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest, OCommandResultListener {

  public static final String NAME = "DELETE FROM";
  public static final String KEYWORD_DELETE = "DELETE";
  private static final String VALUE_NOT_FOUND = "_not_found_";

  private OSQLQuery<EntityImpl> query;
  private String indexName = null;
  private int recordCount = 0;
  private String returning = "COUNT";
  private List<Record> allDeletedRecords;

  private SQLFilter compiledFilter;
  private boolean unsafe = false;

  public CommandExecutorSQLDelete() {
  }

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLDelete parse(final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      final var database = getDatabase();

      init((CommandRequestText) iRequest);

      query = null;
      recordCount = 0;

      if (parserTextUpperCase.endsWith(KEYWORD_UNSAFE)) {
        unsafe = true;
        parserText = parserText.substring(0, parserText.length() - KEYWORD_UNSAFE.length() - 1);
        parserTextUpperCase =
            parserTextUpperCase.substring(
                0, parserTextUpperCase.length() - KEYWORD_UNSAFE.length() - 1);
      }

      parserRequiredKeyword(CommandExecutorSQLDelete.KEYWORD_DELETE);
      parserRequiredKeyword(CommandExecutorSQLDelete.KEYWORD_FROM);

      String subjectName = parserRequiredWord(false, "Syntax error", " =><,\r\n");
      if (subjectName == null) {
        throwSyntaxErrorException(
            "Invalid subject name. Expected cluster, class, index or sub-query");
      }

      if (OStringParser.startsWithIgnoreCase(
          subjectName, CommandExecutorSQLAbstract.INDEX_PREFIX)) {
        // INDEX
        indexName = subjectName.substring(CommandExecutorSQLAbstract.INDEX_PREFIX.length());

        if (!parserIsEnded()) {
          while (!parserIsEnded()) {
            final String word = parserGetLastWord();

            if (word.equals(KEYWORD_RETURN)) {
              returning = parseReturn();
            } else if (word.equals(KEYWORD_UNSAFE)) {
              unsafe = true;
            } else if (word.equalsIgnoreCase(KEYWORD_WHERE)) {
              compiledFilter =
                  OSQLEngine
                      .parseCondition(
                          parserText.substring(parserGetCurrentPosition()),
                          getContext(),
                          KEYWORD_WHERE);
            }

            parserNextWord(true);
          }

        } else {
          parserSetCurrentPosition(-1);
        }

      } else if (subjectName.startsWith("(")) {
        subjectName = subjectName.trim();
        query =
            database.command(
                new OSQLAsynchQuery<EntityImpl>(
                    subjectName.substring(1, subjectName.length() - 1), this));
        parserNextWord(true);
        if (!parserIsEnded()) {
          while (!parserIsEnded()) {
            final String word = parserGetLastWord();

            if (word.equals(KEYWORD_RETURN)) {
              returning = parseReturn();
            } else if (word.equals(KEYWORD_UNSAFE)) {
              unsafe = true;
            } else if (word.equalsIgnoreCase(KEYWORD_WHERE)) {
              compiledFilter =
                  OSQLEngine
                      .parseCondition(
                          parserText.substring(parserGetCurrentPosition()),
                          getContext(),
                          KEYWORD_WHERE);
            }

            parserNextWord(true);
          }
        }
      } else {
        parserNextWord(true);

        while (!parserIsEnded()) {
          final String word = parserGetLastWord();

          if (word.equals(KEYWORD_RETURN)) {
            returning = parseReturn();
          } else {
            parserGoBack();
            break;
          }

          parserNextWord(true);
        }

        final String condition =
            parserGetCurrentPosition() > -1
                ? " " + parserText.substring(parserGetCurrentPosition())
                : "";
        query =
            database.command(
                new OSQLAsynchQuery<EntityImpl>(
                    "select from " + getSelectTarget(subjectName) + condition, this));
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  private String getSelectTarget(String subjectName) {
    if (preParsedStatement == null) {
      return subjectName;
    }
    return ((SQLDeleteStatement) preParsedStatement).fromClause.toString();
  }

  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (query == null && indexName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    if (!returning.equalsIgnoreCase("COUNT")) {
      allDeletedRecords = new ArrayList<Record>();
    }

    if (query != null) {
      // AGAINST CLUSTERS AND CLASSES
      query.setContext(getContext());

      Object prevLockValue = query.getContext().getVariable("$locking");

      query.execute(querySession, iArgs);

      query.getContext().setVariable("$locking", prevLockValue);

      if (returning.equalsIgnoreCase("COUNT"))
      // RETURNS ONLY THE COUNT
      {
        return recordCount;
      } else
      // RETURNS ALL THE DELETED RECORDS
      {
        return allDeletedRecords;
      }

    } else {
      // AGAINST INDEXES
      if (compiledFilter != null) {
        compiledFilter.bindParameters(iArgs);
      }

      final YTDatabaseSessionInternal database = getDatabase();
      final OIndexInternal index =
          database
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(database, indexName)
              .getInternal();
      if (index == null) {
        throw new YTCommandExecutionException("Target index '" + indexName + "' not found");
      }

      OIndexAbstract.manualIndexesWarning();

      Object key = null;
      Object value = VALUE_NOT_FOUND;

      if (compiledFilter == null || compiledFilter.getRootCondition() == null) {
        if (returning.equalsIgnoreCase("COUNT")) {
          // RETURNS ONLY THE COUNT
          final long total = index.size(database);
          index.clear(database);
          return total;
        } else {
          // RETURNS ALL THE DELETED RECORDS
          Iterator<ORawPair<Object, YTRID>> cursor = index.stream(database).iterator();

          while (cursor.hasNext()) {
            final ORawPair<Object, YTRID> entry = cursor.next();
            YTIdentifiable rec = entry.second;
            rec = rec.getRecord();
            if (rec != null) {
              allDeletedRecords.add((Record) rec);
            }
          }

          index.clear(database);

          return allDeletedRecords;
        }

      } else {
        if (KEYWORD_KEY.equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
        // FOUND KEY ONLY
        {
          key =
              getIndexKey(
                  database, index.getDefinition(), compiledFilter.getRootCondition().getRight());
        } else if (KEYWORD_RID.equalsIgnoreCase(
            compiledFilter.getRootCondition().getLeft().toString())) {
          // BY RID
          value = OSQLHelper.getValue(compiledFilter.getRootCondition().getRight());

        } else if (compiledFilter.getRootCondition().getLeft()
            instanceof OSQLFilterCondition leftCondition) {
          // KEY AND VALUE
          if (KEYWORD_KEY.equalsIgnoreCase(leftCondition.getLeft().toString())) {
            key = getIndexKey(database, index.getDefinition(), leftCondition.getRight());
          }

          final OSQLFilterCondition rightCondition =
              (OSQLFilterCondition) compiledFilter.getRootCondition().getRight();
          if (KEYWORD_RID.equalsIgnoreCase(rightCondition.getLeft().toString())) {
            value = OSQLHelper.getValue(rightCondition.getRight());
          }
        }

        final boolean result;
        if (value != VALUE_NOT_FOUND) {
          assert key != null;
          result = index.remove(database, key, (YTIdentifiable) value);
        } else {
          result = index.remove(database, key);
        }

        if (returning.equalsIgnoreCase("COUNT")) {
          return result ? 1 : 0;
        } else
        // TODO: REFACTOR INDEX TO RETURN DELETED ITEMS
        {
          throw new UnsupportedOperationException();
        }
      }
    }
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT);
  }

  /**
   * Deletes the current record.
   */
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    final RecordAbstract record = ((YTIdentifiable) iRecord).getRecord();

    if (record instanceof EntityImpl
        && compiledFilter != null
        && !Boolean.TRUE.equals(
        this.compiledFilter.evaluate(record, (EntityImpl) record, getContext()))) {
      return true;
    }
    if (record.getIdentity().isValid()) {
      if (returning.equalsIgnoreCase("BEFORE")) {
        allDeletedRecords.add(record);
      }

      // RESET VERSION TO DISABLE MVCC AVOIDING THE CONCURRENT EXCEPTION IF LOCAL CACHE IS NOT
      // UPDATED
      //        ORecordInternal.setVersion(record, -1);

      if (!unsafe && record instanceof EntityImpl) {
        // CHECK IF ARE VERTICES OR EDGES
        final YTClass cls = ODocumentInternal.getImmutableSchemaClass(((EntityImpl) record));
        if (cls != null) {
          if (cls.isSubClassOf("V"))
          // FOUND VERTEX
          {
            throw new YTCommandExecutionException(
                "'DELETE' command cannot delete vertices. Use 'DELETE VERTEX' command instead, or"
                    + " apply the 'UNSAFE' keyword to force it");
          } else if (cls.isSubClassOf("E"))
          // FOUND EDGE
          {
            throw new YTCommandExecutionException(
                "'DELETE' command cannot delete edges. Use 'DELETE EDGE' command instead, or"
                    + " apply the 'UNSAFE' keyword to force it");
          }
        }
      }

      record.delete();

      recordCount++;
      return true;
    }
    return false;
  }

  public String getSyntax() {
    return "DELETE FROM <Class>|RID|cluster:<cluster> [UNSAFE] [LOCK <NONE|RECORD>] [RETURN"
        + " <COUNT|BEFORE>] [WHERE <condition>*]";
  }

  @Override
  public void end() {
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected String parseReturn() throws YTCommandSQLParsingException {
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

  private Object getIndexKey(
      YTDatabaseSessionInternal session, final OIndexDefinition indexDefinition, Object value) {
    if (indexDefinition instanceof OCompositeIndexDefinition) {
      if (value instanceof List<?> values) {
        List<Object> keyParams = new ArrayList<Object>(values.size());

        for (Object o : values) {
          keyParams.add(OSQLHelper.getValue(o));
        }
        return indexDefinition.createValue(session, keyParams);
      } else {
        value = OSQLHelper.getValue(value);
        if (value instanceof OCompositeKey) {
          return value;
        } else {
          return indexDefinition.createValue(session, value);
        }
      }
    } else {
      return OSQLHelper.getValue(value);
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE
  getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
    // ALWAYS EXECUTE THE COMMAND LOCALLY BECAUSE THERE IS NO A DISTRIBUTED UNDO WITH SHARDING
    //
    // return (indexName != null || query != null) && !getDatabase().getTransaction().isActive() ?
    // DISTRIBUTED_EXECUTION_MODE.REPLICATE
    // : DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    return DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
    // return getDistributedExecutionMode() == DISTRIBUTED_EXECUTION_MODE.LOCAL ?
    // DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS
    // : DISTRIBUTED_RESULT_MGMT.MERGE;
  }

  @Override
  public Object getResult() {
    return null;
  }
}
