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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * SQL ALTER DATABASE command: Changes an attribute of the current database.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterDatabase extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_DATABASE = "DATABASE";

  private ATTRIBUTES attribute;
  private String value;

  public OCommandExecutorSQLAlterDatabase parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_ALTER)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_DATABASE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1) {
        throw new YTCommandSQLParsingException(
            "Missed the database's attribute to change. Use " + getSyntax(), parserText, oldPos);
      }

      final String attributeAsString = word.toString();

      try {
        attribute =
            ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw YTException.wrapException(
            new YTCommandSQLParsingException(
                "Unknown database's attribute '"
                    + attributeAsString
                    + "'. Supported attributes are: "
                    + Arrays.toString(ATTRIBUTES.values()),
                parserText,
                oldPos),
            e);
      }

      value = parserText.substring(pos + 1).trim();

      if (value.length() == 0) {
        throw new YTCommandSQLParsingException(
            "Missed the database's value to change for attribute '"
                + attribute
                + "'. Use "
                + getSyntax(),
            parserText,
            oldPos);
      }

      if (value.equalsIgnoreCase("null")) {
        value = null;
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
        .getValueAsLong(YTGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  /**
   * Execute the ALTER DATABASE.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (attribute == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final YTDatabaseSessionInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_UPDATE);

    database.setInternal(attribute, value);
    return null;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  public String getSyntax() {
    return "ALTER DATABASE <attribute-name> <attribute-value>";
  }
}
