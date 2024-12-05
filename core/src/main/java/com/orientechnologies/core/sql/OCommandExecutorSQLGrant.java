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
package com.orientechnologies.core.sql;

import com.orientechnologies.core.command.OCommandRequest;
import com.orientechnologies.core.command.OCommandRequestText;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.security.ORole;
import java.util.Map;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 */
public class OCommandExecutorSQLGrant extends OCommandExecutorSQLPermissionAbstract {

  public static final String KEYWORD_GRANT = "GRANT";
  private static final String KEYWORD_TO = "TO";

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLGrant parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      privilege = ORole.PERMISSION_NONE;
      resource = null;
      role = null;

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_GRANT)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_GRANT + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1) {
        throw new YTCommandSQLParsingException("Invalid privilege", parserText, oldPos);
      }

      parsePrivilege(word, oldPos);

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_ON)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_ON + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserText, pos, word, true);
      if (pos == -1) {
        throw new YTCommandSQLParsingException("Invalid resource", parserText, oldPos);
      }

      resource = word.toString();

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_TO)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_TO + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserText, pos, word, true);
      if (pos == -1) {
        throw new YTCommandSQLParsingException("Invalid role", parserText, oldPos);
      }

      final String roleName = word.toString();
      role = getDatabase().getMetadata().getSecurity().getRole(roleName);
      if (role == null) {
        throw new YTCommandSQLParsingException("Invalid role: " + roleName);
      }

    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the GRANT.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (role == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    var db = getDatabase();
    role.grant(db, resource, privilege);
    role.save(db);

    return role;
  }

  public String getSyntax() {
    return "GRANT <permission> ON <resource> TO <role>";
  }
}
