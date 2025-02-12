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
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCreateClassStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL CREATE CLASS command: Creates a new property in the target class.
 */
@SuppressWarnings("unchecked")
public class CommandExecutorSQLCreateClass extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_CLASS = "CLASS";
  public static final String KEYWORD_EXTENDS = "EXTENDS";
  public static final String KEYWORD_ABSTRACT = "ABSTRACT";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_CLUSTERS = "CLUSTERS";
  public static final String KEYWORD_IF = "IF";
  public static final String KEYWORD_NOT = "NOT";
  public static final String KEYWORD_EXISTS = "EXISTS";

  private String className;
  private final List<SchemaClass> superClasses = new ArrayList<SchemaClass>();
  private int[] clusterIds;
  private Integer clusters = null;
  private boolean ifNotExists = false;

  public CommandExecutorSQLCreateClass parse(DatabaseSessionInternal session,
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
      if (pos == -1 || !word.toString().equals(KEYWORD_CREATE)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_CREATE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLASS)) {
        throw new CommandSQLParsingException(session,
            "Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1) {
        throw new CommandSQLParsingException(session, "Expected <class>", parserText, oldPos);
      }

      className = word.toString();
      if (this.preParsedStatement != null) {
        className = ((SQLCreateClassStatement) preParsedStatement).name.getStringValue();
      }
      if (className == null) {
        throw new CommandSQLParsingException(session, "Expected <class>", parserText, oldPos);
      }

      oldPos = pos;

      while ((pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true)) > -1) {
        final var k = word.toString();
        switch (k) {
          case KEYWORD_EXTENDS -> {
            boolean hasNext;
            var newParser = this.preParsedStatement != null;
            SchemaClass superClass;
            do {
              oldPos = pos;
              pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
              if (pos == -1) {
                throw new CommandSQLParsingException(session,
                    "Syntax error after EXTENDS for class "
                        + className
                        + ". Expected the super-class name. Use "
                        + getSyntax(),
                    parserText, oldPos);
              }
              var superclassName = decodeClassName(word.toString());

              if (!session.getMetadata().getImmutableSchemaSnapshot().existsClass(superclassName)
                  && !newParser) {
                throw new CommandSQLParsingException(session,
                    "Super-class " + word + " not exists", parserText, oldPos);
              }
              superClass = session.getMetadata().getSchema().getClass(superclassName);
              superClasses.add(superClass);
              hasNext = false;
              for (; pos < parserText.length(); pos++) {
                var ch = parserText.charAt(pos);
                if (ch == ',') {
                  hasNext = true;
                } else if (Character.isLetterOrDigit(ch)) {
                  break;
                } else if (ch == '`') {
                  break;
                }
              }
            } while (hasNext);
            if (newParser) {
              var statement = (SQLCreateClassStatement) this.preParsedStatement;
              var superclasses = statement.getSuperclasses();
              this.superClasses.clear();
              for (var superclass : superclasses) {
                var superclassName = superclass.getStringValue();
                if (!session.getMetadata().getSchema().existsClass(superclassName)) {
                  throw new CommandSQLParsingException(session,
                      "Super-class " + word + " not exists", parserText, oldPos);
                }
                superClass = session.getMetadata().getSchema().getClass(superclassName);
                this.superClasses.add(superClass);
              }
            }
          }
          case KEYWORD_CLUSTER -> {
            oldPos = pos;
            pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
            if (pos == -1) {
              throw new CommandSQLParsingException(session,
                  "Syntax error after CLUSTER for class "
                      + className
                      + ". Expected the cluster id or name. Use "
                      + getSyntax(),
                  parserText, oldPos);
            }

            final var clusterIdsAsStrings = word.toString().split(",");
            if (clusterIdsAsStrings.length > 0) {
              clusterIds = new int[clusterIdsAsStrings.length];
              for (var i = 0; i < clusterIdsAsStrings.length; ++i) {
                if (Character.isDigit(clusterIdsAsStrings[i].charAt(0)))
                // GET CLUSTER ID FROM NAME
                {
                  clusterIds[i] = Integer.parseInt(clusterIdsAsStrings[i]);
                } else
                // GET CLUSTER ID
                {
                  clusterIds[i] = session.getClusterIdByName(clusterIdsAsStrings[i]);
                }

                if (clusterIds[i] == -1) {
                  throw new CommandSQLParsingException(session,
                      "Cluster with id " + clusterIds[i] + " does not exists", parserText, oldPos);
                }

                try {
                  var clusterName = session.getClusterNameById(clusterIds[i]);
                  if (clusterName == null) {
                    throw new ClusterDoesNotExistException(session.getDatabaseName(),
                        "Cluster with id "
                            + clusterIds[i]
                            + " does not exist inside of storage "
                            + session.getDatabaseName());
                  }
                } catch (Exception e) {
                  throw BaseException.wrapException(
                      new CommandSQLParsingException(session,
                          "Cluster with id " + clusterIds[i] + " does not exists",
                          parserText, oldPos),
                      e, session);
                }
              }
            }
          }
          case KEYWORD_CLUSTERS -> {
            oldPos = pos;
            pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
            if (pos == -1) {
              throw new CommandSQLParsingException(session,
                  "Syntax error after CLUSTERS for class "
                      + className
                      + ". Expected the number of clusters. Use "
                      + getSyntax(),
                  parserText, oldPos);
            }

            clusters = Integer.parseInt(word.toString());
          }
          case KEYWORD_ABSTRACT -> clusterIds = new int[]{-1};
          case KEYWORD_IF -> {
            oldPos = pos;
            pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
            if (!word.toString().equalsIgnoreCase(KEYWORD_NOT)) {
              throw new CommandSQLParsingException(session,
                  "Syntax error after IF for class "
                      + className
                      + ". Expected NOT. Use "
                      + getSyntax(),
                  parserText, oldPos);
            }
            oldPos = pos;
            pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
            if (!word.toString().equalsIgnoreCase(KEYWORD_EXISTS)) {
              throw new CommandSQLParsingException(session,
                  "Syntax error after IF NOT for class "
                      + className
                      + ". Expected EXISTS. Use "
                      + getSyntax(),
                  parserText, oldPos);
            }
            ifNotExists = true;
          }
          default -> throw new CommandSQLParsingException(session.getDatabaseName(),
              "Invalid keyword: " + k);
        }

        oldPos = pos;
      }

      if (clusterIds == null) {
        final var clusterId = session.getClusterIdByName(className);
        if (clusterId > -1) {
          clusterIds = new int[]{clusterId};
        }
      }

    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  /**
   * Execute the CREATE CLASS.
   */
  public Object execute(DatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    if (className == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    var alreadyExists = session.getMetadata().getSchema().existsClass(className);
    if (!alreadyExists || !ifNotExists) {
      if (clusters != null) {
        session
            .getMetadata()
            .getSchema()
            .createClass(className, clusters, superClasses.toArray(new SchemaClass[0]));
      } else {
        session
            .getMetadata()
            .getSchema()
            .createClass(className, clusterIds, superClasses.toArray(new SchemaClass[0]));
      }
    }

    return session.getMetadata().getSchema().getClasses().size();
  }

  @Override
  public String getSyntax() {
    return "CREATE CLASS <class> [IF NOT EXISTS] [EXTENDS <super-class> [,<super-class2>*] ]"
        + " [CLUSTER <clusterId>*] [CLUSTERS <total-cluster-number>] [ABSTRACT]";
  }

  public String getUndoCommand() {
    return "drop class " + className;
  }

}
