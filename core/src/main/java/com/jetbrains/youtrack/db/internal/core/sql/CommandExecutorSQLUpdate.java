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

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.common.util.OTriple;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurity;
import com.jetbrains.youtrack.db.internal.core.query.Query;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItem;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUpdateStatement;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.storage.YTRecordDuplicatedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * SQL UPDATE command.
 */
public class CommandExecutorSQLUpdate extends CommandExecutorSQLRetryAbstract
    implements OCommandDistributedReplicateRequest, OCommandResultListener {

  public static final String KEYWORD_UPDATE = "UPDATE";
  private static final String KEYWORD_ADD = "ADD";
  private static final String KEYWORD_PUT = "PUT";
  private static final String KEYWORD_REMOVE = "REMOVE";
  private static final String KEYWORD_INCREMENT = "INCREMENT";
  private static final String KEYWORD_MERGE = "MERGE";
  private static final String KEYWORD_UPSERT = "UPSERT";
  private static final String KEYWORD_EDGE = "EDGE";
  private static final Object EMPTY_VALUE = new Object();
  private final List<OPair<String, Object>> setEntries = new ArrayList<OPair<String, Object>>();
  private final List<OPair<String, Object>> addEntries = new ArrayList<OPair<String, Object>>();
  private final List<OTriple<String, String, Object>> putEntries =
      new ArrayList<OTriple<String, String, Object>>();
  private final List<OPair<String, Object>> removeEntries = new ArrayList<OPair<String, Object>>();
  private final List<OPair<String, Object>> incrementEntries =
      new ArrayList<OPair<String, Object>>();
  private EntityImpl merge = null;
  private OReturnHandler returnHandler = new ORecordCountHandler();
  private Query<?> query;
  private SQLFilter compiledFilter;
  private String subjectName;
  private OCommandParameters parameters;
  private boolean upsertMode = false;
  private boolean isUpsertAllowed = false;
  private boolean updated = false;
  private YTClass clazz = null;
  private DISTRIBUTED_EXECUTION_MODE distributedMode;

  private boolean updateEdge = false;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLUpdate parse(final CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      if (updateEdge) {
        queryText =
            queryText.replaceFirst(
                "EDGE ", ""); // work-around to use UPDATE syntax without having to
      }
      textRequest.setText(queryText);

      final var database = getDatabase();

      init((CommandRequestText) iRequest);

      setEntries.clear();
      addEntries.clear();
      putEntries.clear();
      removeEntries.clear();
      incrementEntries.clear();
      content = null;
      merge = null;

      query = null;

      parserRequiredKeyword(KEYWORD_UPDATE);

      subjectName = parserRequiredWord(false, "Invalid target", " =><,\r\n");
      if (subjectName == null) {
        throwSyntaxErrorException(
            "Invalid subject name. Expected cluster, class, index or sub-query");
      }
      if (subjectName.equalsIgnoreCase("EDGE")) {
        updateEdge = true;
        subjectName = parserRequiredWord(false, "Invalid target", " =><,\r\n");
      }

      clazz = extractClassFromTarget(subjectName);

      String word = parserNextWord(true);

      if (parserIsEnded()
          || (!word.equals(KEYWORD_SET)
          && !word.equals(KEYWORD_ADD)
          && !word.equals(KEYWORD_PUT)
          && !word.equals(KEYWORD_REMOVE)
          && !word.equals(KEYWORD_INCREMENT)
          && !word.equals(KEYWORD_CONTENT)
          && !word.equals(KEYWORD_MERGE)
          && !word.equals(KEYWORD_RETURN)
          && !word.equals(KEYWORD_UPSERT)
          && !word.equals(KEYWORD_EDGE))) {
        throwSyntaxErrorException(
            "Expected keyword "
                + KEYWORD_SET
                + ","
                + KEYWORD_ADD
                + ","
                + KEYWORD_CONTENT
                + ","
                + KEYWORD_MERGE
                + ","
                + KEYWORD_PUT
                + ","
                + KEYWORD_REMOVE
                + ","
                + KEYWORD_INCREMENT
                + " or "
                + KEYWORD_RETURN
                + " or "
                + KEYWORD_UPSERT
                + " or "
                + KEYWORD_EDGE);
      }

      while ((!parserIsEnded()
          && !parserGetLastWord().equals(CommandExecutorSQLAbstract.KEYWORD_WHERE))
          || parserGetLastWord().equals(KEYWORD_UPSERT)) {
        word = parserGetLastWord();

        if (word.equals(KEYWORD_CONTENT)) {
          parseContent();
        } else if (word.equals(KEYWORD_MERGE)) {
          parseMerge();
        } else if (word.equals(KEYWORD_SET)) {
          parseSetFields(clazz, setEntries);
        } else if (word.equals(KEYWORD_ADD)) {
          parseAddFields();
        } else if (word.equals(KEYWORD_PUT)) {
          parsePutFields();
        } else if (word.equals(KEYWORD_REMOVE)) {
          parseRemoveFields();
        } else if (word.equals(KEYWORD_INCREMENT)) {
          parseIncrementFields();
        } else if (word.equals(KEYWORD_UPSERT)) {
          upsertMode = true;
        } else if (word.equals(KEYWORD_RETURN)) {
          parseReturn();
        } else if (word.equals(KEYWORD_RETRY)) {
          LogManager.instance().warn(this, "RETRY keyword will be ignored in " + originalQuery);
          parseRetry();
        } else {
          break;
        }

        parserNextWord(true);
      }

      final String additionalStatement = parserGetLastWord();

      if (subjectName.startsWith("(")) {
        subjectName = subjectName.trim();
        query =
            database.command(
                new OSQLAsynchQuery<EntityImpl>(
                    subjectName.substring(1, subjectName.length() - 1), this)
                    .setContext(context));

        if (additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_WHERE)
            || additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_LIMIT)) {
          compiledFilter =
              OSQLEngine
                  .parseCondition(
                      parserText.substring(parserGetCurrentPosition()),
                      getContext(),
                      KEYWORD_WHERE);
        }

      } else if (additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_WHERE)
          || additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_LIMIT)
          || additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_LET)) {
        if (this.preParsedStatement != null) {
          Map<Object, Object> params = ((CommandRequestText) iRequest).getParameters();
          SQLUpdateStatement updateStm = (SQLUpdateStatement) preParsedStatement;
          StringBuilder selectString = new StringBuilder();
          selectString.append("select from ");
          updateStm.target.toString(params, selectString);
          if (updateStm.whereClause != null) {
            selectString.append(" WHERE ");
            updateStm.whereClause.toString(params, selectString);
          }
          if (updateStm.limit != null) {
            selectString.append(" ");
            updateStm.limit.toString(params, selectString);
          }
          if (updateStm.timeout != null) {
            selectString.append(" ");
            updateStm.timeout.toString(params, selectString);
          }

          query = new OSQLAsynchQuery<EntityImpl>(selectString.toString(), this);
        } else {
          query =
              new OSQLAsynchQuery<EntityImpl>(
                  "select from "
                      + getSelectTarget()
                      + " "
                      + additionalStatement
                      + " "
                      + parserText.substring(parserGetCurrentPosition()),
                  this);
        }

        isUpsertAllowed =
            (getDatabase().getMetadata().getImmutableSchemaSnapshot().getClass(subjectName)
                != null);
      } else if (!additionalStatement.isEmpty()) {
        throwSyntaxErrorException("Invalid keyword " + additionalStatement);
      } else {
        query = new OSQLAsynchQuery<EntityImpl>("select from " + getSelectTarget(), this);
      }

      if (upsertMode && !isUpsertAllowed) {
        throwSyntaxErrorException("Upsert only works with class names ");
      }

      if (upsertMode && !additionalStatement.equals(CommandExecutorSQLAbstract.KEYWORD_WHERE)) {
        throwSyntaxErrorException("Upsert only works with WHERE keyword");
      }
      if (upsertMode && updateEdge) {
        throwSyntaxErrorException("Upsert is not supported with UPDATE EDGE");
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  private boolean isUpdateEdge() {
    return updateEdge;
  }

  private String getSelectTarget() {
    if (preParsedStatement == null) {
      return subjectName;
    }
    return ((SQLUpdateStatement) preParsedStatement).target.toString();
  }

  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (subjectName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    parameters = new OCommandParameters(iArgs);
    Map<Object, Object> queryArgs;
    if (parameters.size() > 0 && parameters.getByName(0) != null) {
      queryArgs = new HashMap<Object, Object>();
      for (int i = parameterCounter; i < parameters.size(); i++) {
        if (parameters.getByName(i) != null) {
          queryArgs.put(i - parameterCounter, parameters.getByName(i));
        }
      }
    } else {
      queryArgs = iArgs;
    }

    query.setContext(context);

    returnHandler.reset();

    getDatabase().query(query, queryArgs);

    if (upsertMode && !updated) {
      // IF UPDATE DOES NOT PRODUCE RESULTS AND UPSERT MODE IS ENABLED, CREATE DOCUMENT AND APPLY
      // SET/ADD/PUT/MERGE and so on
      final EntityImpl doc =
          subjectName != null ? new EntityImpl(subjectName) : new EntityImpl();
      // locks by result(doc)
      try {
        result(querySession, doc);
      } catch (YTRecordDuplicatedException e) {
        if (upsertMode)
        // UPDATE THE NEW RECORD
        {
          getDatabase().query(query, queryArgs);
        } else {
          throw e;
        }
      } catch (YTRecordNotFoundException e) {
        if (upsertMode)
        // UPDATE THE NEW RECORD
        {
          getDatabase().query(query, queryArgs);
        } else {
          throw e;
        }
      } catch (YTConcurrentModificationException e) {
        if (upsertMode)
        // UPDATE THE NEW RECORD
        {
          getDatabase().query(query, queryArgs);
        } else {
          throw e;
        }
      }
    }

    return returnHandler.ret();
  }

  /**
   * Update current record.
   */
  @SuppressWarnings("unchecked")
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    final EntityImpl record = ((YTIdentifiable) iRecord).getRecord();

    if (updateEdge && !isRecordInstanceOf(iRecord, "E")) {
      throw new YTCommandExecutionException(
          "Using UPDATE EDGE on a record that is not an instance of E");
    }
    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(record, null, context)) {
        return false;
      }
    }

    parameters.reset();

    returnHandler.beforeUpdate(record);

    boolean updated = handleContent(record);
    updated |= handleMerge(record);
    updated |= handleSetEntries(record);
    updated |= handleIncrementEntries(record);
    updated |= handleAddEntries(querySession, record);
    updated |= handlePutEntries(querySession, record);
    updated |= handleRemoveEntries(querySession, record);

    if (updated) {
      handleUpdateEdge(querySession, record);
      record.setDirty();
      record.save();
      returnHandler.afterUpdate(record);
      this.updated = true;
    }

    return true;
  }

  /**
   * checks if an object is an YTIdentifiable and an instance of a particular (schema) class
   *
   * @param iRecord     The record object
   * @param orientClass The schema class
   * @return
   */
  private boolean isRecordInstanceOf(Object iRecord, String orientClass) {
    if (iRecord == null) {
      return false;
    }
    if (!(iRecord instanceof YTIdentifiable)) {
      return false;
    }
    EntityImpl record = ((YTIdentifiable) iRecord).getRecord();
    if (iRecord == null) {
      return false;
    }
    return (ODocumentInternal.getImmutableSchemaClass(record).isSubClassOf(orientClass));
  }

  /**
   * handles vertex consistency after an UPDATE EDGE
   *
   * @param db
   * @param record the edge record
   */
  private void handleUpdateEdge(YTDatabaseSessionInternal db, EntityImpl record) {
    if (!updateEdge) {
      return;
    }
    Object currentOut = record.field("out");
    Object currentIn = record.field("in");

    Object prevOut = record.getOriginalValue("out");
    Object prevIn = record.getOriginalValue("in");

    validateOutInForEdge(record, currentOut, currentIn);

    changeVertexEdgePointer(db, record, (YTIdentifiable) prevIn, (YTIdentifiable) currentIn, "in");
    changeVertexEdgePointer(db, record, (YTIdentifiable) prevOut, (YTIdentifiable) currentOut,
        "out");
  }

  /**
   * updates old and new vertices connected to an edge after out/in update on the edge itself
   *
   * @param db
   * @param edge          the edge
   * @param prevVertex    the previously connected vertex
   * @param currentVertex the currently connected vertex
   * @param direction     the direction ("out" or "in")
   */
  private void changeVertexEdgePointer(
      YTDatabaseSessionInternal db, EntityImpl edge, YTIdentifiable prevVertex,
      YTIdentifiable currentVertex, String direction) {
    if (prevVertex != null && !prevVertex.equals(currentVertex)) {
      String edgeClassName = edge.getClassName();
      if (edgeClassName.equalsIgnoreCase("E")) {
        edgeClassName = "";
      }
      String vertexFieldName = direction + "_" + edgeClassName;
      EntityImpl prevOutDoc = prevVertex.getRecord();
      RidBag prevBag = prevOutDoc.field(vertexFieldName);
      if (prevBag != null) {
        prevBag.remove(edge);
        prevOutDoc.save();
      }

      EntityImpl currentVertexDoc = currentVertex.getRecord();
      RidBag currentBag = currentVertexDoc.field(vertexFieldName);
      if (currentBag == null) {
        currentBag = new RidBag(db);
        currentVertexDoc.field(vertexFieldName, currentBag);
      }
      currentBag.add(edge);
    }
  }

  private void validateOutInForEdge(EntityImpl record, Object currentOut, Object currentIn) {
    if (!isRecordInstanceOf(currentOut, "V")) {
      throw new YTCommandExecutionException(
          "Error updating edge: 'out' is not a vertex - " + currentOut);
    }
    if (!isRecordInstanceOf(currentIn, "V")) {
      throw new YTCommandExecutionException(
          "Error updating edge: 'in' is not a vertex - " + currentIn);
    }
  }

  @Override
  public String getSyntax() {
    return "UPDATE <class>|cluster:<cluster>> [SET|ADD|PUT|REMOVE|INCREMENT|CONTENT {<JSON>}|MERGE"
        + " {<JSON>}] [[,] <field-name> = <expression>|<sub-command>]* [LOCK <NONE|RECORD>]"
        + " [UPSERT] [RETURN <COUNT|BEFORE|AFTER>] [WHERE <conditions>]";
  }

  @Override
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE
  getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
    // ALWAYS EXECUTE THE COMMAND LOCALLY BECAUSE THERE IS NO A DISTRIBUTED UNDO WITH SHARDING
    /*
    if (distributedMode == null)
      // REPLICATE MODE COULD BE MORE EFFICIENT ON MASSIVE UPDATES
      distributedMode = upsertMode || query == null || getDatabase().getTransaction().isActive() ?
          DISTRIBUTED_EXECUTION_MODE.LOCAL :
          DISTRIBUTED_EXECUTION_MODE.REPLICATE;
    return distributedMode;*/
  }

  @Override
  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    return DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
  }

  @Override
  public void end() {
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_UPDATE;
  }

  protected void parseMerge() {
    if (!parserIsEnded() && !parserGetLastWord().equals(KEYWORD_WHERE)) {
      final String contentAsString = parserRequiredWord(false, "document to merge expected").trim();
      merge = new EntityImpl();
      merge.fromJSON(contentAsString);
      parserSkipWhiteSpaces();
    }

    if (merge == null) {
      throwSyntaxErrorException(
          "Document to merge not provided. Example: MERGE { \"name\": \"Jay\" }");
    }
  }

  protected String getBlock(String fieldValue) {
    final int startPos = parserGetCurrentPosition();

    if (fieldValue.startsWith("{") || fieldValue.startsWith("[")) {
      if (startPos > 0) {
        parserSetCurrentPosition(startPos - fieldValue.length());
      } else {
        parserSetCurrentPosition(parserText.length() - fieldValue.length());
      }

      parserSkipWhiteSpaces();
      final StringBuilder buffer = new StringBuilder();
      parserSetCurrentPosition(
          OStringSerializerHelper.parse(
              parserText,
              buffer,
              parserGetCurrentPosition(),
              -1,
              OStringSerializerHelper.DEFAULT_FIELD_SEPARATOR,
              true,
              true,
              false,
              -1,
              false,
              OStringSerializerHelper.DEFAULT_IGNORE_CHARS));
      fieldValue = buffer.toString();
    }
    return fieldValue;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected void parseReturn() throws YTCommandSQLParsingException {
    parserNextWord(false, " ");
    String mode = parserGetLastWord().trim();

    if (mode.equalsIgnoreCase("COUNT")) {
      returnHandler = new ORecordCountHandler();
    } else if (mode.equalsIgnoreCase("BEFORE") || mode.equalsIgnoreCase("AFTER")) {

      parserNextWord(false, " ");
      String returning = parserGetLastWord().trim();
      Object returnExpression = null;
      if (returning.equalsIgnoreCase(KEYWORD_WHERE)
          || returning.equalsIgnoreCase(KEYWORD_TIMEOUT)
          || returning.equalsIgnoreCase(KEYWORD_LIMIT)
          || returning.equalsIgnoreCase(KEYWORD_UPSERT)
          || returning.length() == 0) {
        parserGoBack();
      } else {
        if (returning.startsWith("$") || returning.startsWith("@")) {
          returnExpression =
              (returning.length() > 0)
                  ? OSQLHelper.parseValue(this, returning, this.getContext())
                  : null;
        } else {
          throwSyntaxErrorException(
              "record attribute (@attributes) or functions with $current variable expected");
        }
      }

      if (mode.equalsIgnoreCase("BEFORE")) {
        returnHandler = new OOriginalRecordsReturnHandler(returnExpression, getContext());
      } else {
        returnHandler = new OUpdatedRecordsReturnHandler(returnExpression, getContext());
      }

    } else {
      throwSyntaxErrorException(" COUNT | BEFORE | AFTER keywords expected");
    }
  }

  private boolean handleContent(EntityImpl record) {
    boolean updated = false;
    if (content != null) {
      // REPLACE ALL THE CONTENT
      final EntityImpl fieldsToPreserve = new EntityImpl();

      final YTClass restricted =
          getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(OSecurity.RESTRICTED_CLASSNAME);

      if (restricted != null
          && restricted.isSuperClassOf(ODocumentInternal.getImmutableSchemaClass(record))) {
        for (YTProperty prop : restricted.properties(getDatabase())) {
          fieldsToPreserve.field(prop.getName(), record.<Object>field(prop.getName()));
        }
      }

      YTClass recordClass = ODocumentInternal.getImmutableSchemaClass(record);
      if (recordClass != null && recordClass.isSubClassOf("V")) {
        for (String fieldName : record.fieldNames()) {
          if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
            fieldsToPreserve.field(fieldName, record.<Object>field(fieldName));
          }
        }
      } else if (recordClass != null && recordClass.isSubClassOf("E")) {
        for (String fieldName : record.fieldNames()) {
          if (fieldName.equals("in") || fieldName.equals("out")) {
            fieldsToPreserve.field(fieldName, record.<Object>field(fieldName));
          }
        }
      }
      record.merge(fieldsToPreserve, false, false);
      record.merge(content, true, false);

      updated = true;
    }
    return updated;
  }

  private boolean handleMerge(EntityImpl record) {
    boolean updated = false;
    if (merge != null) {
      // MERGE THE CONTENT
      record.merge(merge, true, false);
      updated = true;
    }
    return updated;
  }

  private boolean handleSetEntries(final EntityImpl record) {
    boolean updated = false;
    // BIND VALUES TO UPDATE
    if (!setEntries.isEmpty()) {
      OSQLHelper.bindParameters(record, setEntries, parameters, context);
      updated = true;
    }
    return updated;
  }

  private boolean handleIncrementEntries(final EntityImpl record) {
    boolean updated = false;
    // BIND VALUES TO INCREMENT
    if (!incrementEntries.isEmpty()) {
      for (OPair<String, Object> entry : incrementEntries) {
        final Number prevValue = record.field(entry.getKey());

        Number current;
        if (entry.getValue() instanceof OSQLFilterItem) {
          current = (Number) ((OSQLFilterItem) entry.getValue()).getValue(record, null, context);
        } else if (entry.getValue() instanceof Number) {
          current = (Number) entry.getValue();
        } else {
          throw new YTCommandExecutionException(
              "Increment value is not a number (" + entry.getValue() + ")");
        }

        if (prevValue == null)
        // NO PREVIOUS VALUE: CONSIDER AS 0
        {
          record.field(entry.getKey(), current);
        } else
        // COMPUTING INCREMENT
        {
          record.field(entry.getKey(), YTType.increment(prevValue, current));
        }
      }
      updated = true;
    }
    return updated;
  }

  private boolean handleAddEntries(YTDatabaseSessionInternal querySession, EntityImpl record) {
    boolean updated = false;
    // BIND VALUES TO ADD
    Object fieldValue;
    for (OPair<String, Object> entry : addEntries) {
      Collection<Object> coll = null;
      RidBag bag = null;
      if (!record.containsField(entry.getKey())) {
        // GET THE TYPE IF ANY
        if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
          YTProperty prop =
              ODocumentInternal.getImmutableSchemaClass(record).getProperty(entry.getKey());
          if (prop != null && prop.getType() == YTType.LINKSET)
          // SET TYPE
          {
            coll = new HashSet<Object>();
          }
          if (prop != null && prop.getType() == YTType.LINKBAG) {
            // there is no ridbag value already but property type is defined as LINKBAG
            bag = new RidBag(querySession);
            bag.setOwner(record);
            record.field(entry.getKey(), bag);
          }
        }
        if (coll == null && bag == null)
        // IN ALL OTHER CASES USE A LIST
        {
          coll = new ArrayList<Object>();
        }
        if (coll != null) {
          // containField's condition above does NOT check subdocument's fields so
          Collection<Object> currColl = record.field(entry.getKey());
          if (currColl == null) {
            record.field(entry.getKey(), coll);
            coll = record.field(entry.getKey());
          } else {
            coll = currColl;
          }
        }

      } else {
        fieldValue = record.field(entry.getKey());

        if (fieldValue instanceof Collection<?>) {
          coll = (Collection<Object>) fieldValue;
        } else if (fieldValue instanceof RidBag) {
          bag = (RidBag) fieldValue;
        } else {
          continue;
        }
      }

      final Object value = extractValue(querySession, record, entry);

      if (coll != null) {
        if (value instanceof YTIdentifiable) {
          coll.add(value);
        } else {
          OMultiValue.add(coll, value);
        }
      } else {
        if (!(value instanceof YTIdentifiable)) {
          throw new YTCommandExecutionException("Only links or records can be added to LINKBAG");
        }

        bag.add((YTIdentifiable) value);
      }
      updated = true;
    }
    return updated;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private boolean handlePutEntries(YTDatabaseSessionInternal querySession, EntityImpl record) {
    boolean updated = false;
    if (!putEntries.isEmpty()) {
      // BIND VALUES TO PUT (AS MAP)
      for (OTriple<String, String, Object> entry : putEntries) {
        Object fieldValue = record.field(entry.getKey());

        if (fieldValue == null) {
          if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
            final YTProperty property =
                ODocumentInternal.getImmutableSchemaClass(record).getProperty(entry.getKey());
            if (property != null
                && (property.getType() != null
                && (!property.getType().equals(YTType.EMBEDDEDMAP)
                && !property.getType().equals(YTType.LINKMAP)))) {
              throw new YTCommandExecutionException(
                  "field " + entry.getKey() + " is not defined as a map");
            }
          }
          fieldValue = new HashMap<String, Object>();
          record.field(entry.getKey(), fieldValue);
        }

        if (fieldValue instanceof Map<?, ?>) {
          Map<String, Object> map = (Map<String, Object>) fieldValue;

          OPair<String, Object> pair = entry.getValue();

          Object value = extractValue(querySession, record, pair);

          if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
            final YTProperty property =
                ODocumentInternal.getImmutableSchemaClass(record).getProperty(entry.getKey());
            if (property != null
                && property.getType().equals(YTType.LINKMAP)
                && !(value instanceof YTIdentifiable)) {
              throw new YTCommandExecutionException(
                  "field " + entry.getKey() + " defined of type LINKMAP accept only link values");
            }
          }
          if (YTType.LINKMAP.equals(YTType.getTypeByValue(fieldValue))
              && !(value instanceof YTIdentifiable)) {
            map = new TrackedMap(record, map, Object.class);
            record.field(entry.getKey(), map, YTType.EMBEDDEDMAP);
          }
          map.put(pair.getKey(), value);
          updated = true;
        }
      }
    }
    return updated;
  }

  private boolean handleRemoveEntries(YTDatabaseSessionInternal querySession, EntityImpl record) {
    boolean updated = false;
    if (!removeEntries.isEmpty()) {
      // REMOVE FIELD IF ANY
      for (OPair<String, Object> entry : removeEntries) {
        Object value = extractValue(querySession, record, entry);

        if (value == EMPTY_VALUE) {
          record.removeField(entry.getKey());
          updated = true;
        } else {
          final Object fieldValue = record.field(entry.getKey());

          if (fieldValue instanceof Collection<?>) {
            updated = removeFromCollection(updated, value, (Collection<?>) fieldValue);
          } else if (fieldValue instanceof Map<?, ?>) {
            updated = removeFromMap(updated, value, (Map<?, ?>) fieldValue);
          } else if (fieldValue instanceof RidBag) {
            updated = removeFromBag(record, updated, value, (RidBag) fieldValue);
          }
        }
      }
    }
    return updated;
  }

  private boolean removeFromCollection(boolean updated, Object value, Collection<?> collection) {
    if (value instanceof Collection<?>) {
      updated |= collection.removeAll(((Collection) value));
    } else {
      updated |= collection.remove(value);
    }
    return updated;
  }

  private boolean removeFromMap(boolean updated, Object value, Map<?, ?> map) {
    if (value instanceof Collection) {
      for (Object o : ((Collection) value)) {
        updated |= map.remove(o) != null;
      }
    } else {
      updated |= map.remove(value) != null;
    }
    return updated;
  }

  private boolean removeFromBag(EntityImpl record, boolean updated, Object value, RidBag bag) {
    if (value instanceof Collection) {
      for (Object o : ((Collection) value)) {
        updated |= removeSingleValueFromBag(bag, o, record);
      }
    } else {
      updated |= removeSingleValueFromBag(bag, value, record);
    }
    return updated;
  }

  private boolean removeSingleValueFromBag(RidBag bag, Object value, EntityImpl record) {
    if (!(value instanceof YTIdentifiable)) {
      throw new YTCommandExecutionException("Only links or records can be removed from LINKBAG");
    }

    bag.remove((YTIdentifiable) value);
    return record.isDirty();
  }

  private Object extractValue(YTDatabaseSessionInternal requestSession, EntityImpl record,
      OPair<String, Object> entry) {
    Object value = entry.getValue();

    if (value instanceof OSQLFilterItem) {
      value = ((OSQLFilterItem) value).getValue(record, null, context);
    } else if (value instanceof CommandRequest) {
      value = ((CommandRequest) value).execute(requestSession, record, null, context);
    }

    if (value instanceof YTIdentifiable && ((YTIdentifiable) value).getIdentity().isPersistent())
    // USE ONLY THE RID TO AVOID CONCURRENCY PROBLEM WITH OLD VERSIONS
    {
      value = ((YTIdentifiable) value).getIdentity();
    }
    return value;
  }

  private void parseAddFields() {
    String fieldName;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded()
        && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      final Object v = convertValue(clazz, fieldName, getFieldValueCountingParameters(fieldValue));

      // INSERT TRANSFORMED FIELD VALUE
      addEntries.add(new OPair<String, Object>(fieldName, v));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (addEntries.size() == 0) {
      throwSyntaxErrorException(
          "Entries to add <field> = <value> are missed. Example: name = 'Bill', salary = 300.2.");
    }
  }

  private void parsePutFields() {
    String fieldName;
    String fieldKey;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded()
        && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldKey = parserRequiredWord(false, "Key expected");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));

      // INSERT TRANSFORMED FIELD VALUE
      putEntries.add(
          new OTriple<>(
              fieldName,
              (String) getFieldValueCountingParameters(fieldKey),
              getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (putEntries.isEmpty()) {
      throwSyntaxErrorException(
          "Entries to put <field> = <key>, <value> are missed. Example: name = 'Bill', 30");
    }
  }

  private void parseRemoveFields() {
    String fieldName;
    String fieldValue;
    Object value;

    boolean firstLap = true;
    while (!parserIsEnded()
        && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      final boolean found = parserOptionalKeyword("=", "WHERE");
      if (found) {
        if (parserGetLastWord().equals("WHERE")) {
          parserGoBack();
          value = EMPTY_VALUE;
        } else {
          fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));
          value = getFieldValueCountingParameters(fieldValue);
        }
      } else {
        value = EMPTY_VALUE;
      }

      // INSERT FIELD NAME TO BE REMOVED
      removeEntries.add(new OPair<String, Object>(fieldName, value));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (removeEntries.size() == 0) {
      throwSyntaxErrorException("Field(s) to remove are missed. Example: name, salary");
    }
  }

  private void parseIncrementFields() {
    String fieldName;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded()
        && (firstLap || parserGetLastSeparator() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected"));

      // INSERT TRANSFORMED FIELD VALUE
      incrementEntries.add(new OPair(fieldName, getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (incrementEntries.size() == 0) {
      throwSyntaxErrorException(
          "Entries to increment <field> = <value> are missed. Example: salary = -100");
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public Object getResult() {
    return null;
  }
}
