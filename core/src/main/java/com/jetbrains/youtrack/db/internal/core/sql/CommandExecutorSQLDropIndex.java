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
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import java.util.Map;

/**
 * SQL REMOVE INDEX command: Remove an index
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLDropIndex extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_INDEX = "INDEX";

  private String name;

  public CommandExecutorSQLDropIndex parse(DatabaseSessionInternal db,
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

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_INDEX)) {
        throw new CommandSQLParsingException(
            "Keyword " + KEYWORD_INDEX + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1) {
        throw new CommandSQLParsingException(
            "Expected index name. Use " + getSyntax(), parserText, oldPos);
      }

      name = word.toString();
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the REMOVE INDEX.
   */
  public Object execute(DatabaseSessionInternal db, final Map<Object, Object> iArgs) {
    if (name == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final var database = getDatabase();
    if (name.equals("*")) {
      long totalIndexed = 0;
      for (var idx : database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
        database.getMetadata().getIndexManagerInternal().dropIndex(database, idx.getName());
        totalIndexed++;
      }

      return totalIndexed;

    } else {
      database.getMetadata().getIndexManagerInternal().dropIndex(database, name);
    }

    return 1;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public String getSyntax() {
    return "DROP INDEX <index-name>|<class>.<property>|*";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
