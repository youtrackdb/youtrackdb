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

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
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
  public CommandExecutorSQLTruncateCluster parse(DatabaseSessionInternal session,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);

      init(session, (CommandRequestText) iRequest);

      var word = new StringBuilder();

      var oldPos = 0;
      var pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserText, oldPos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(session,
            "Expected cluster name. Use " + getSyntax(), parserText, oldPos);
      }

      clusterName = decodeClusterName(word.toString());

      if (preParsedStatement
          != null) { // new parser, this will be removed and implemented with the new executor
        var name = ((SQLTruncateClusterStatement) preParsedStatement).clusterName;
        if (name != null) {
          clusterName = name.getStringValue();
        }
      }

      if (session.getClusterIdByName(clusterName) == -1) {
        throw new CommandSQLParsingException(session,
            "Cluster '" + clusterName + "' not found", parserText, oldPos);
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  private static String decodeClusterName(String s) {
    return decodeClassName(s);
  }

  /**
   * Execute the command.
   */
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (clusterName == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    final var clusterId = session.getClusterIdByName(clusterName);
    if (clusterId < 0) {
      throw new DatabaseException(session, "Cluster with name " + clusterName + " does not exist");
    }

    var schema = session.getMetadata().getSchemaInternal();
    var clazz = (SchemaClassInternal) schema.getClassByClusterId(clusterId);
    if (clazz == null) {
      session.checkForClusterPermissions(clusterName);

      final var iteratorCluster = session.browseCluster(clusterName);
      if (iteratorCluster == null) {
        throw new DatabaseException(session,
            "Cluster with name " + clusterName + " does not exist");
      }
      while (iteratorCluster.hasNext()) {
        final var record = iteratorCluster.next();
        record.delete();
      }
    } else {
      clazz.truncateCluster(session, clusterName);
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE CLUSTER <cluster-name>";
  }
}
