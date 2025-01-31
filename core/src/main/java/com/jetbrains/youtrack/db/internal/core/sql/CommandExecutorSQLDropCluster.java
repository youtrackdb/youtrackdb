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
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.Map;

/**
 * SQL DROP CLUSTER command: Drop a cluster from the database
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLDropCluster extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  private String clusterName;

  public CommandExecutorSQLDropCluster parse(DatabaseSessionInternal db,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      final var word = new StringBuilder();

      var oldPos = 0;
      var pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DROP)) {
        throw new CommandSQLParsingException(
            "Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER)) {
        throw new CommandSQLParsingException(
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
      if (pos == -1) {
        throw new CommandSQLParsingException(
            "Expected <cluster>. Use " + getSyntax(), parserText, pos);
      }

      clusterName = word.toString();
      if (clusterName == null) {
        throw new CommandSQLParsingException(
            "Cluster is null. Use " + getSyntax(), parserText, pos);
      }

      clusterName = decodeClassName(clusterName);
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the DROP CLUSTER.
   */
  public Object execute(DatabaseSessionInternal db, final Map<Object, Object> iArgs) {
    if (clusterName == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    // CHECK IF ANY CLASS IS USING IT
    final var clusterId = db.getClusterIdByName(clusterName);
    for (var iClass : db.getMetadata().getSchema().getClasses(db)) {
      for (var i : iClass.getClusterIds()) {
        if (i == clusterId)
        // IN USE
        {
          return false;
        }
      }
    }

    db.dropCluster(clusterId);
    return true;
  }

  @Override
  public long getDistributedTimeout() {
    if (clusterName != null && getDatabase().existsCluster(clusterName)) {
      return 10 * getDatabase().countClusterElements(clusterName);
    }

    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "DROP CLUSTER <cluster>";
  }
}
