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

import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL CREATE FUNCTION command.
 */
public class CommandExecutorSQLCreateFunction extends CommandExecutorSQLAbstract {

  public static final String NAME = "CREATE FUNCTION";
  private String name;
  private String code;
  private String language;
  private boolean idempotent = false;
  private List<String> parameters = null;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLCreateFunction parse(final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      parserRequiredKeyword("CREATE");
      parserRequiredKeyword("FUNCTION");

      name = parserNextWord(false);
      code = IOUtils.getStringContent(parserNextWord(false));

      String temp = parseOptionalWord(true);
      while (temp != null) {
        if (temp.equals("IDEMPOTENT")) {
          parserNextWord(false);
          idempotent = Boolean.parseBoolean(parserGetLastWord());
        } else if (temp.equals("LANGUAGE")) {
          parserNextWord(false);
          language = parserGetLastWord();
        } else if (temp.equals("PARAMETERS")) {
          parserNextWord(false);
          parameters = new ArrayList<String>();
          StringSerializerHelper.getCollection(parserGetLastWord(), 0, parameters);
          if (parameters.size() == 0) {
            throw new CommandExecutionException(
                "Syntax Error. Missing function parameter(s): " + getSyntax());
          }
        }

        temp = parserOptionalWord(true);
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

  /**
   * Execute the command and return the EntityImpl object created.
   */
  public Object execute(final Map<Object, Object> iArgs, DatabaseSessionInternal querySession) {
    if (name == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }
    if (name.isEmpty()) {
      throw new CommandExecutionException(
          "Syntax Error. You must specify a function name: " + getSyntax());
    }
    if (code == null || code.isEmpty()) {
      throw new CommandExecutionException(
          "Syntax Error. You must specify the function code: " + getSyntax());
    }

    var database = getDatabase();
    final Function f = database.getMetadata().getFunctionLibrary().createFunction(name);
    f.setCode(database, code);
    f.setIdempotent(database, idempotent);
    if (parameters != null) {
      f.setParameters(database, parameters);
    }
    if (language != null) {
      f.setLanguage(database, language);
    }

    f.save(database);
    return f.getId(database);
  }

  @Override
  public String getSyntax() {
    return "CREATE FUNCTION <name> <code> [PARAMETERS [<comma-separated list of parameters' name>]]"
        + " [IDEMPOTENT true|false] [LANGUAGE <language>]";
  }
}
