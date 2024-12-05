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
import java.util.Map;

/**
 * SQL CREATE CLUSTER command: Creates a new cluster.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateCluster extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_BLOB = "BLOB";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_ID = "ID";

  private String clusterName;
  private int requestedId = -1;
  private boolean blob = false;

  public OCommandExecutorSQLCreateCluster parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final YTDatabaseSessionInternal database = getDatabase();

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword(KEYWORD_CREATE);
      String nextWord = parserRequiredWord(true);
      if (nextWord.equals("BLOB")) {
        parserRequiredKeyword(KEYWORD_CLUSTER);
        blob = true;
      } else if (!nextWord.equals(KEYWORD_CLUSTER)) {
        throw new YTCommandSQLParsingException("Invalid Syntax: " + queryText);
      }

      clusterName = parserRequiredWord(false);
      clusterName = decodeClassName(clusterName);
      if (!clusterName.isEmpty() && Character.isDigit(clusterName.charAt(0))) {
        throw new IllegalArgumentException("Cluster name cannot begin with a digit");
      }

      String temp = parseOptionalWord(true);

      while (temp != null) {
        if (temp.equals(KEYWORD_ID)) {
          requestedId = Integer.parseInt(parserRequiredWord(false));
        }

        temp = parseOptionalWord(true);
        if (parserIsEnded()) {
          break;
        }
      }

    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  /**
   * Execute the CREATE CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (clusterName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final var database = getDatabase();

    final int clusterId = database.getClusterIdByName(clusterName);
    if (clusterId > -1) {
      throw new YTCommandSQLParsingException("Cluster '" + clusterName + "' already exists");
    }

    if (blob) {
      if (requestedId == -1) {
        return database.addBlobCluster(clusterName);
      } else {
        throw new YTCommandExecutionException("Request id not supported by blob cluster creation.");
      }
    } else {
      if (requestedId == -1) {
        return database.addCluster(clusterName);
      } else {
        return database.addCluster(clusterName, requestedId);
      }
    }
  }

  @Override
  public String getUndoCommand() {
    return "drop cluster " + clusterName;
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> [ID <requested cluster id>]";
  }
}
