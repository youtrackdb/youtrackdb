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
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import java.util.Map;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 */
public class CommandExecutorSQLGrant extends CommandExecutorSQLPermissionAbstract {

  public static final String KEYWORD_GRANT = "GRANT";
  private static final String KEYWORD_TO = "TO";

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLGrant parse(DatabaseSessionInternal session,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);

      init(session, (CommandRequestText) iRequest);

      privilege = Role.PERMISSION_NONE;
      resource = null;
      role = null;

      var word = new StringBuilder();

      var oldPos = 0;
      var pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_GRANT)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_GRANT + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(session, "Invalid privilege", parserText, oldPos);
      }

      parsePrivilege(word, oldPos);

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_ON)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_ON + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserText, pos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(session, "Invalid resource", parserText, oldPos);
      }

      resource = word.toString();

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_TO)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_TO + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserText, pos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(session, "Invalid role", parserText, oldPos);
      }

      final var roleName = word.toString();
      role = session.getMetadata().getSecurity().getRole(roleName);
      if (role == null) {
        throw new CommandSQLParsingException(session.getDatabaseName(),
            "Invalid role: " + roleName);
      }

    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the GRANT.
   */
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (role == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    role.grant(session, resource, privilege);
    role.save(session);

    return role;
  }

  public String getSyntax() {
    return "GRANT <permission> ON <resource> TO <role>";
  }
}
