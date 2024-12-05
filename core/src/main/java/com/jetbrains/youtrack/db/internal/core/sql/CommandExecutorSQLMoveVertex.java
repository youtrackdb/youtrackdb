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

import com.jetbrains.youtrack.db.internal.common.types.OModifiableBoolean;
import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL MOVE VERTEX command.
 */
public class CommandExecutorSQLMoveVertex extends CommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest {

  public static final String NAME = "MOVE VERTEX";
  private static final String KEYWORD_MERGE = "MERGE";
  private static final String KEYWORD_BATCH = "BATCH";
  private String source = null;
  private String clusterName;
  private String className;
  private YTClass clazz;
  private List<OPair<String, Object>> fields;
  private EntityImpl merge;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLMoveVertex parse(final CommandRequest iRequest) {
    final var database = getDatabase();

    init((CommandRequestText) iRequest);

    parserRequiredKeyword("MOVE");
    parserRequiredKeyword("VERTEX");

    source = parserRequiredWord(false, "Syntax error", " =><,\r\n");
    if (source == null) {
      throw new YTCommandSQLParsingException("Cannot find source");
    }

    parserRequiredKeyword("TO");

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.startsWith("CLUSTER:")) {
        if (className != null) {
          throw new YTCommandSQLParsingException(
              "Cannot define multiple sources. Found both cluster and class.");
        }

        clusterName = temp.substring("CLUSTER:".length());
        if (database.getClusterIdByName(clusterName) == -1) {
          throw new YTCommandSQLParsingException("Cluster '" + clusterName + "' was not found");
        }

      } else if (temp.startsWith("CLASS:")) {
        if (clusterName != null) {
          throw new YTCommandSQLParsingException(
              "Cannot define multiple sources. Found both cluster and class.");
        }

        className = temp.substring("CLASS:".length());

        clazz = database.getMetadata().getSchema().getClass(className);

        if (clazz == null) {
          throw new YTCommandSQLParsingException("Class '" + className + "' was not found");
        }

      } else if (temp.equals(KEYWORD_SET)) {
        fields = new ArrayList<OPair<String, Object>>();
        parseSetFields(clazz, fields);

      } else if (temp.equals(KEYWORD_MERGE)) {
        merge = parseJSON();

      } else if (temp.equals(KEYWORD_BATCH)) {
        temp = parserNextWord(true);
        if (temp != null) {
          batch = Integer.parseInt(temp);
        }
      }

      temp = parserOptionalWord(true);
      if (parserIsEnded()) {
        break;
      }
    }

    return this;
  }

  /**
   * Executes the command and return the EntityImpl object created.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {

    YTDatabaseSessionInternal db = getDatabase();

    db.begin();

    if (className == null && clusterName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    OModifiableBoolean shutdownGraph = new OModifiableBoolean();
    final boolean txAlreadyBegun = getDatabase().getTransaction().isActive();

    final Set<YTIdentifiable> sourceRIDs =
        OSQLEngine.getInstance().parseRIDTarget(db, source, context, iArgs);

    // CREATE EDGES
    final List<EntityImpl> result = new ArrayList<EntityImpl>(sourceRIDs.size());

    for (YTIdentifiable from : sourceRIDs) {
      final Vertex fromVertex = toVertex(from);
      if (fromVertex == null) {
        continue;
      }

      final YTRID oldVertex = fromVertex.getIdentity().copy();
      final YTRID newVertex = fromVertex.moveTo(className, clusterName);

      final EntityImpl newVertexDoc = newVertex.getRecord();

      if (fields != null) {
        // EVALUATE FIELDS
        for (final OPair<String, Object> f : fields) {
          if (f.getValue() instanceof OSQLFunctionRuntime) {
            f.setValue(
                ((OSQLFunctionRuntime) f.getValue())
                    .getValue(newVertex.getRecord(), null, context));
          }
        }

        OSQLHelper.bindParameters(newVertexDoc, fields, new OCommandParameters(iArgs), context);
      }

      if (merge != null) {
        newVertexDoc.merge(merge, true, false);
      }

      // SAVE CHANGES
      newVertexDoc.save();

      // PUT THE MOVE INTO THE RESULT
      result.add(
          new EntityImpl()
              .setTrackingChanges(false)
              .field("old", oldVertex, YTType.LINK)
              .field("new", newVertex, YTType.LINK));

      if (batch > 0 && result.size() % batch == 0) {
        db.commit();
        db.begin();
      }
    }

    db.commit();

    return result;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public String getSyntax() {
    return "MOVE VERTEX <source> TO <destination> [SET [<field>=<value>]* [,]] [MERGE <JSON>]"
        + " [BATCH <batch-size>]";
  }

  private static Vertex toVertex(YTIdentifiable item) {
    if (item instanceof Entity) {
      return ((Entity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }
      if (item instanceof Entity) {
        return ((Entity) item).asVertex().orElse(null);
      }
    }
    return null;
  }
}
