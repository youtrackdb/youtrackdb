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
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.Map;

/**
 * SQL CREATE CLUSTER command: Creates a new cluster.
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLCreateCluster extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_BLOB = "BLOB";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_ID = "ID";

  private String clusterName;
  private int requestedId = -1;
  private boolean blob = false;

  public CommandExecutorSQLCreateCluster parse(DatabaseSessionInternal session,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);

      init(session, (CommandRequestText) iRequest);

      parserRequiredKeyword(session.getDatabaseName(), KEYWORD_CREATE);
      var nextWord = parserRequiredWord(session.getDatabaseName(), true);
      if (nextWord.equals("BLOB")) {
        parserRequiredKeyword(session.getDatabaseName(), KEYWORD_CLUSTER);
        blob = true;
      } else if (!nextWord.equals(KEYWORD_CLUSTER)) {
        throw new CommandSQLParsingException(session.getDatabaseName(),
            "Invalid Syntax: " + queryText);
      }

      clusterName = parserRequiredWord(session.getDatabaseName(), false);
      clusterName = decodeClassName(clusterName);
      if (!clusterName.isEmpty() && Character.isDigit(clusterName.charAt(0))) {
        throw new IllegalArgumentException("Cluster name cannot begin with a digit");
      }

      var temp = parseOptionalWord(session.getDatabaseName(), true);

      while (temp != null) {
        if (temp.equals(KEYWORD_ID)) {
          requestedId = Integer.parseInt(parserRequiredWord(session.getDatabaseName(), false));
        }

        temp = parseOptionalWord(session.getDatabaseName(), true);
        if (parserIsEnded()) {
          break;
        }
      }

    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the CREATE CLUSTER.
   */
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (clusterName == null) {
      throw new CommandExecutionException(session.getDatabaseName(),
          "Cannot execute the command because it has not been parsed yet");
    }

    final var clusterId = session.getClusterIdByName(clusterName);
    if (clusterId > -1) {
      throw new CommandSQLParsingException(session.getDatabaseName(),
          "Cluster '" + clusterName + "' already exists");
    }

    if (blob) {
      if (requestedId == -1) {
        return session.addBlobCluster(clusterName);
      } else {
        throw new CommandExecutionException(session.getDatabaseName(),
            "Request id not supported by blob cluster creation.");
      }
    } else {
      if (requestedId == -1) {
        return session.addCluster(clusterName);
      } else {
        return session.addCluster(clusterName, requestedId);
      }
    }
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> [ID <requested cluster id>]";
  }
}
