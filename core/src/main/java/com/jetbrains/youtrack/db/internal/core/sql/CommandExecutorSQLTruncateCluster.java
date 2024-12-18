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
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLTruncateClusterStatement;
import java.util.Map;

/**
 * SQL TRUNCATE CLUSTER command: Truncates an entire record cluster.
 */
public class CommandExecutorSQLTruncateCluster extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_TRUNCATE = "TRUNCATE";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  private String clusterName;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLTruncateCluster parse(DatabaseSessionInternal db,
      final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE)) {
        throw new CommandSQLParsingException(
            "Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER)) {
        throw new CommandSQLParsingException(
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserText, oldPos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(
            "Expected cluster name. Use " + getSyntax(), parserText, oldPos);
      }

      clusterName = decodeClusterName(word.toString());

      if (preParsedStatement
          != null) { // new parser, this will be removed and implemented with the new executor
        SQLIdentifier name = ((SQLTruncateClusterStatement) preParsedStatement).clusterName;
        if (name != null) {
          clusterName = name.getStringValue();
        }
      }

      final var database = getDatabase();
      if (database.getClusterIdByName(clusterName) == -1) {
        throw new CommandSQLParsingException(
            "Cluster '" + clusterName + "' not found", parserText, oldPos);
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  private String decodeClusterName(String s) {
    return decodeClassName(s);
  }

  /**
   * Execute the command.
   */
  public Object execute(DatabaseSessionInternal db, final Map<Object, Object> iArgs) {
    if (clusterName == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final DatabaseSessionInternal database = getDatabase();

    final int clusterId = database.getClusterIdByName(clusterName);
    if (clusterId < 0) {
      throw new DatabaseException("Cluster with name " + clusterName + " does not exist");
    }

    var schema = database.getMetadata().getSchemaInternal();
    var clazz = (SchemaClassInternal) schema.getClassByClusterId(clusterId);
    if (clazz == null) {
      database.checkForClusterPermissions(clusterName);

      final RecordIteratorCluster<Record> iteratorCluster = database.browseCluster(clusterName);
      if (iteratorCluster == null) {
        throw new DatabaseException("Cluster with name " + clusterName + " does not exist");
      }
      while (iteratorCluster.hasNext()) {
        final Record record = iteratorCluster.next();
        record.delete();
      }
    } else {
      clazz.truncateCluster(database, clusterName);
    }
    return true;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE CLUSTER <cluster-name>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }
}
