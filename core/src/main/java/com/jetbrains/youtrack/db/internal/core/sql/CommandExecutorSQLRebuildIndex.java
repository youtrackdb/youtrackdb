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
 * SQL REMOVE INDEX command: Remove an index
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLRebuildIndex extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_REBUILD = "REBUILD";
  public static final String KEYWORD_INDEX = "INDEX";

  private String name;

  public CommandExecutorSQLRebuildIndex parse(DatabaseSessionInternal session,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);
      init(session, (CommandRequestText) iRequest);

      final var word = new StringBuilder();

      var oldPos = 0;
      var pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_REBUILD)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_REBUILD + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_INDEX)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_INDEX + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1) {
        throw new CommandSQLParsingException(session, "Expected index name", parserText, oldPos);
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
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (name == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    if (name.equals("*")) {
      long totalIndexed = 0;
      for (var idx : session.getMetadata().getIndexManagerInternal().getIndexes(session)) {
        if (idx.isAutomatic()) {
          totalIndexed += idx.rebuild(session);
        }
      }

      return totalIndexed;

    } else {
      final var idx = session.getMetadata().getIndexManagerInternal().getIndex(session, name);
      if (idx == null) {
        throw new CommandExecutionException(session, "Index '" + name + "' not found");
      }

      if (!idx.isAutomatic()) {
        throw new CommandExecutionException(session,
            "Cannot rebuild index '"
                + name
                + "' because it's manual and there aren't indications of what to index");
      }

      return idx.rebuild(session);
    }
  }

  @Override
  public String getSyntax() {
    return "REBUILD INDEX <index-name>";
  }
}
