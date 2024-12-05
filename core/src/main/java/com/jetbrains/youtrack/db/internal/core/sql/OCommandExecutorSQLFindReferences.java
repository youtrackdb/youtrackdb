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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FIND REFERENCES command: Finds references to records in all or part of database
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLFindReferences extends OCommandExecutorSQLEarlyResultsetAbstract {

  public static final String KEYWORD_FIND = "FIND";
  public static final String KEYWORD_REFERENCES = "REFERENCES";

  private final Set<YTRID> recordIds = new HashSet<YTRID>();
  private String classList;
  private StringBuilder subQuery;

  public OCommandExecutorSQLFindReferences parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;
    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword(KEYWORD_FIND);
      parserRequiredKeyword(KEYWORD_REFERENCES);
      final String target = parserRequiredWord(true, "Expected <target>", " =><,\r\n");

      if (target.charAt(0) == '(') {
        subQuery = new StringBuilder();
        parserSetCurrentPosition(
            OStringSerializerHelper.getEmbedded(
                parserText, parserGetPreviousPosition(), -1, subQuery));
      } else {
        try {
          final YTRecordId rid = new YTRecordId(target);
          if (!rid.isValid()) {
            throwParsingException("Record ID " + target + " is not valid");
          }
          recordIds.add(rid);

        } catch (IllegalArgumentException iae) {
          throw YTException.wrapException(
              new YTCommandSQLParsingException(
                  "Error reading record Id", parserText, parserGetPreviousPosition()),
              iae);
        }
      }

      parserSkipWhiteSpaces();
      classList = parserOptionalWord(true);
      if (classList != null) {
        classList = parserTextUpperCase.substring(parserGetPreviousPosition());

        if (!classList.startsWith("[") || !classList.endsWith("]")) {
          throwParsingException("Class list must be contained in []");
        }
        // GET THE CLUSTER LIST TO SEARCH, IF NULL WILL SEARCH ENTIRE DATABASE
        classList = classList.substring(1, classList.length() - 1);
      }

      return this;
    } finally {
      textRequest.setText(originalQuery);
    }
  }

  /**
   * Execute the FIND REFERENCES.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (recordIds.isEmpty() && subQuery == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    if (subQuery != null) {
      final List<YTIdentifiable> result = new OCommandSQL(subQuery.toString()).execute(
          querySession);
      for (YTIdentifiable id : result) {
        recordIds.add(id.getIdentity());
      }
    }

    return OFindReferenceHelper.findReferences(recordIds, classList);
  }

  @Override
  public String getSyntax() {
    return "FIND REFERENCES <rid|<sub-query>> [class-list]";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }
}
