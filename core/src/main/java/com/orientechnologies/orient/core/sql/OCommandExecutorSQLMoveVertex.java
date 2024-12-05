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

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL MOVE VERTEX command.
 */
public class OCommandExecutorSQLMoveVertex extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest {

  public static final String NAME = "MOVE VERTEX";
  private static final String KEYWORD_MERGE = "MERGE";
  private static final String KEYWORD_BATCH = "BATCH";
  private String source = null;
  private String clusterName;
  private String className;
  private YTClass clazz;
  private List<OPair<String, Object>> fields;
  private YTEntityImpl merge;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLMoveVertex parse(final OCommandRequest iRequest) {
    final var database = getDatabase();

    init((OCommandRequestText) iRequest);

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
   * Executes the command and return the YTEntityImpl object created.
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
    final List<YTEntityImpl> result = new ArrayList<YTEntityImpl>(sourceRIDs.size());

    for (YTIdentifiable from : sourceRIDs) {
      final YTVertex fromVertex = toVertex(from);
      if (fromVertex == null) {
        continue;
      }

      final YTRID oldVertex = fromVertex.getIdentity().copy();
      final YTRID newVertex = fromVertex.moveTo(className, clusterName);

      final YTEntityImpl newVertexDoc = newVertex.getRecord();

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
          new YTEntityImpl()
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

  private static YTVertex toVertex(YTIdentifiable item) {
    if (item instanceof YTEntity) {
      return ((YTEntity) item).asVertex().orElse(null);
    } else {
      try {
        item = getDatabase().load(item.getIdentity());
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }
      if (item instanceof YTEntity) {
        return ((YTEntity) item).asVertex().orElse(null);
      }
    }
    return null;
  }
}
