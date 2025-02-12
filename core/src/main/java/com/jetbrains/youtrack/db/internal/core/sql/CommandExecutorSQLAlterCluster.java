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

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.ClusterDoesNotExistException;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster.ATTRIBUTES;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLAlterCluster extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  protected String clusterName;
  protected int clusterId = -1;
  protected ATTRIBUTES attribute;
  protected String value;

  public CommandExecutorSQLAlterCluster parse(DatabaseSessionInternal session,
      final CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);

      init(session, (CommandRequestText) iRequest);

      var word = new StringBuilder();

      var oldPos = 0;
      var pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_ALTER)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1) {
        throw new CommandSQLParsingException(session,
            "Expected <cluster-name>. Use " + getSyntax(), parserText, oldPos);
      }

      clusterName = word.toString();
      clusterName = decodeClassName(clusterName);

      final var p = Pattern.compile("([0-9]*)");
      final var m = p.matcher(clusterName);
      if (m.matches()) {
        clusterId = Integer.parseInt(clusterName);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1) {
        throw new CommandSQLParsingException(session,
            "Missing cluster attribute to change. Use " + getSyntax(), parserText, oldPos);
      }

      final var attributeAsString = word.toString();

      try {
        attribute = StorageCluster.ATTRIBUTES.valueOf(
            attributeAsString.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw BaseException.wrapException(
            new CommandSQLParsingException(session,
                "Unknown class attribute '"
                    + attributeAsString
                    + "'. Supported attributes are: "
                    + Arrays.toString(ATTRIBUTES.values()),
                parserText, oldPos),
            e, session);
      }

      value = parserText.substring(pos + 1).trim();

      value = decodeClassName(value);

      if (attribute == ATTRIBUTES.NAME) {
        value = value.replaceAll(" ", ""); // no spaces in cluster names
      }

      if (value.length() == 0) {
        throw new CommandSQLParsingException(session,
            "Missing property value to change for attribute '"
                + attribute
                + "'. Use "
                + getSyntax(),
            parserText, oldPos);
      }

      if (value.equalsIgnoreCase("null")) {
        value = null;
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the ALTER CLASS.
   */
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (attribute == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    final var clusters = getClusters(session);

    if (clusters.isEmpty()) {
      throw new CommandExecutionException(session, "Cluster '" + clusterName + "' not found");
    }

    Object result = null;

    for (final int clusterId : getClusters(session)) {
      if (this.clusterId > -1 && clusterName.equals(String.valueOf(this.clusterId))) {
        clusterName = session.getClusterNameById(clusterId);
        if (clusterName == null) {
          throw new ClusterDoesNotExistException(session.getDatabaseName(),
              "Cluster with id "
                  + clusterId
                  + " does not exist inside of storage "
                  + session.getDatabaseName());
        }
      } else {
        this.clusterId = clusterId;
      }
      final var storage = session.getStorage();
      result = storage.setClusterAttribute(clusterId, attribute, value);
    }

    return result;
  }


  protected IntArrayList getClusters(DatabaseSessionInternal db) {
    final var result = new IntArrayList();

    if (clusterName.endsWith("*")) {
      final var toMatch =
          clusterName.substring(0, clusterName.length() - 1).toLowerCase(Locale.ENGLISH);
      for (var cl : db.getClusterNames()) {
        if (cl.startsWith(toMatch)) {
          result.add(db.getStorage().getClusterIdByName(cl));
        }
      }
    } else {
      if (clusterId > -1) {
        result.add(clusterId);
      } else {
        result.add(db.getStorage().getClusterIdByName(clusterName));
      }
    }

    return result;
  }

  public String getSyntax() {
    return "ALTER CLUSTER <cluster-name>|<cluster-id> <attribute-name> <attribute-value>";
  }
}
