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

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.core.command.OCommandRequest;
import com.orientechnologies.core.command.OCommandRequestText;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClassImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropProperty extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String className;
  private String fieldName;
  private boolean ifExists;
  private boolean force = false;

  public OCommandExecutorSQLDropProperty parse(final OCommandRequest iRequest) {
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
      if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_PROPERTY + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
      if (pos == -1) {
        throw new YTCommandSQLParsingException(
            "Expected <class>.<property>. Use " + getSyntax(), parserText, pos);
      }

      String[] parts = word.toString().split("\\.");
      if (parts.length != 2) {
        throw new YTCommandSQLParsingException(
            "Expected <class>.<property>. Use " + getSyntax(), parserText, pos);
      }

      className = decodeClassName(parts[0]);
      if (className == null) {
        throw new YTCommandSQLParsingException("Class not found", parserText, pos);
      }
      fieldName = decodeClassName(parts[1]);

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
      if (pos != -1) {
        final String forceParameter = word.toString();
        if ("FORCE".equals(forceParameter)) {
          force = true;
        } else if ("IF".contentEquals(word)) {
          pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
          if ("EXISTS".contentEquals(word)) {
            this.ifExists = true;
          } else {
            throw new YTCommandSQLParsingException(
                "Wrong query parameter, expecting EXISTS after IF", parserText, pos);
          }
        } else {
          throw new YTCommandSQLParsingException("Wrong query parameter", parserText, pos);
        }
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (fieldName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not yet been parsed");
    }

    final var database = getDatabase();
    final YTClassImpl sourceClass =
        (YTClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null) {
      throw new YTCommandExecutionException("Source class '" + className + "' not found");
    }

    if (ifExists && !sourceClass.existsProperty(fieldName)) {
      return null;
    }

    final List<OIndex> indexes = relatedIndexes(fieldName);
    if (!indexes.isEmpty()) {
      if (force) {
        dropRelatedIndexes(indexes);
      } else {
        final StringBuilder indexNames = new StringBuilder();

        boolean first = true;
        for (final OIndex index : sourceClass.getClassInvolvedIndexes(database, fieldName)) {
          if (!first) {
            indexNames.append(", ");
          } else {
            first = false;
          }
          indexNames.append(index.getName());
        }

        throw new YTCommandExecutionException(
            "Property used in indexes ("
                + indexNames
                + "). Please drop these indexes before removing property or use FORCE parameter.");
      }
    }

    // REMOVE THE PROPERTY
    sourceClass.dropProperty(database, fieldName);

    return null;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(YTGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  private void dropRelatedIndexes(final List<OIndex> indexes) {
    var database = getDatabase();
    for (final OIndex index : indexes) {
      database.command("DROP INDEX " + index.getName()).close();
    }
  }

  private List<OIndex> relatedIndexes(final String fieldName) {
    final List<OIndex> result = new ArrayList<OIndex>();

    final YTDatabaseSessionInternal database = getDatabase();
    for (final OIndex oIndex :
        database.getMetadata().getIndexManagerInternal().getClassIndexes(database, className)) {
      if (OCollections.indexOf(
          oIndex.getDefinition().getFields(), fieldName, new OCaseInsentiveComparator())
          > -1) {
        result.add(oIndex);
      }
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return "DROP PROPERTY <class>.<property> [ IF EXISTS ] [FORCE]";
  }
}
