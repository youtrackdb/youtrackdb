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

import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestText;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import java.util.Map;

/**
 * SQL DROP CLUSTER command: Drop a cluster from the database
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropCluster extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  private String clusterName;

  public OCommandExecutorSQLDropCluster parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      final StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DROP)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
      if (pos == -1) {
        throw new YTCommandSQLParsingException(
            "Expected <cluster>. Use " + getSyntax(), parserText, pos);
      }

      clusterName = word.toString();
      if (clusterName == null) {
        throw new YTCommandSQLParsingException(
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
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (clusterName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final YTDatabaseSessionInternal database = getDatabase();

    // CHECK IF ANY CLASS IS USING IT
    final int clusterId = database.getClusterIdByName(clusterName);
    for (YTClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
        // IN USE
        {
          return false;
        }
      }
    }

    database.dropCluster(clusterId);
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

  protected boolean isClusterDeletable(int clusterId) {
    final var database = getDatabase();
    for (YTClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP CLUSTER <cluster>";
  }
}
