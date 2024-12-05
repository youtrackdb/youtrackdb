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
package com.jetbrains.youtrack.db.internal.core.sql.filter;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLResultsetDelegate;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQLResultset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Target parser.
 */
public class OSQLTarget extends BaseParser {

  protected final boolean empty;
  protected final CommandContext context;
  protected String targetVariable;
  protected String targetQuery;
  protected Iterable<? extends YTIdentifiable> targetRecords;
  protected Map<String, String> targetClusters;
  protected Map<String, String> targetClasses;

  protected String targetIndex;

  protected String targetIndexValues;
  protected boolean targetIndexValuesAsc;

  public OSQLTarget(final String iText, final CommandContext iContext) {
    super();
    context = iContext;
    parserText = iText;
    parserTextUpperCase = SQLPredicate.upperCase(iText);

    try {
      empty = !extractTargets();

    } catch (YTQueryParsingException e) {
      if (e.getText() == null)
      // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
      {
        throw YTException.wrapException(
            new YTQueryParsingException(
                "Error on parsing query", parserText, parserGetCurrentPosition()),
            e);
      }

      throw e;
    } catch (YTCommandExecutionException ex) {
      throw ex;
    } catch (Exception e) {
      throw YTException.wrapException(
          new YTQueryParsingException(
              "Error on parsing query", parserText, parserGetCurrentPosition()),
          e);
    }
  }

  public Map<String, String> getTargetClusters() {
    return targetClusters;
  }

  public Map<String, String> getTargetClasses() {
    return targetClasses;
  }

  public Iterable<? extends YTIdentifiable> getTargetRecords() {
    return targetRecords;
  }

  public String getTargetQuery() {
    return targetQuery;
  }

  public String getTargetIndex() {
    return targetIndex;
  }

  public String getTargetIndexValues() {
    return targetIndexValues;
  }

  public boolean isTargetIndexValuesAsc() {
    return targetIndexValuesAsc;
  }

  @Override
  public String toString() {
    if (targetClasses != null) {
      return "class " + targetClasses.keySet();
    } else if (targetClusters != null) {
      return "cluster " + targetClusters.keySet();
    }
    if (targetIndex != null) {
      return "index " + targetIndex;
    }
    if (targetRecords != null) {
      return "records from " + targetRecords.getClass().getSimpleName();
    }
    if (targetVariable != null) {
      return "variable " + targetVariable;
    }
    return "?";
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public boolean isEmpty() {
    return empty;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new YTCommandSQLParsingException(
        iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }

  @SuppressWarnings("unchecked")
  private boolean extractTargets() {
    parserSkipWhiteSpaces();

    if (parserIsEnded()) {
      throw new YTQueryParsingException("No query target found", parserText, 0);
    }

    final char c = parserGetCurrentChar();

    if (c == '$') {
      targetVariable = parserRequiredWord(false, "No valid target");
      targetVariable = targetVariable.substring(1);
    } else if (c == OStringSerializerHelper.LINK || Character.isDigit(c)) {
      // UNIQUE RID
      targetRecords = new ArrayList<YTIdentifiable>();
      ((List<YTIdentifiable>) targetRecords)
          .add(new YTRecordId(parserRequiredWord(true, "No valid RID")));

    } else if (c == OStringSerializerHelper.EMBEDDED_BEGIN) {
      // SUB QUERY
      final StringBuilder subText = new StringBuilder(256);
      parserSetCurrentPosition(
          OStringSerializerHelper.getEmbedded(parserText, parserGetCurrentPosition(), -1, subText)
              + 1);
      final CommandSQL subCommand = new CommandSQLResultset(subText.toString());

      YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
      final CommandExecutorSQLResultsetDelegate executor =
          (CommandExecutorSQLResultsetDelegate)
              db.getSharedContext()
                  .getYouTrackDB()
                  .getScriptManager()
                  .getCommandManager()
                  .getExecutor(subCommand);
      executor.setContext(context);
      var commandContext = new BasicCommandContext();
      context.setChild(commandContext);

      subCommand.setContext(commandContext);
      executor.setProgressListener(subCommand.getProgressListener());
      executor.parse(subCommand);
      CommandContext childContext = executor.getContext();
      if (childContext != null) {
        childContext.setParent(context);
      }

      if (!(executor instanceof Iterable<?>)) {
        throw new YTCommandSQLParsingException(
            "Sub-query cannot be iterated because doesn't implement the Iterable interface: "
                + subCommand);
      }

      targetQuery = subText.toString();
      targetRecords = executor;

    } else if (c == OStringSerializerHelper.LIST_BEGIN) {
      // COLLECTION OF RIDS
      final List<String> rids = new ArrayList<String>();
      parserSetCurrentPosition(
          OStringSerializerHelper.getCollection(parserText, parserGetCurrentPosition(), rids));

      targetRecords = new ArrayList<YTIdentifiable>();
      for (String rid : rids) {
        ((List<YTIdentifiable>) targetRecords).add(new YTRecordId(rid));
      }

      parserMoveCurrentPosition(1);
    } else {

      while (!parserIsEnded()
          && (targetClasses == null
          && targetClusters == null
          && targetIndex == null
          && targetIndexValues == null
          && targetRecords == null)) {
        String originalSubjectName = parserRequiredWord(false, "Target not found");
        String subjectName = originalSubjectName.toUpperCase(Locale.ENGLISH);

        final String alias;
        if (subjectName.equals("AS")) {
          alias = parserRequiredWord(true, "Alias not found");
        } else {
          alias = subjectName;
        }

        final String subjectToMatch = subjectName;
        if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
          // REGISTER AS CLUSTER
          if (targetClusters == null) {
            targetClusters = new HashMap<String, String>();
          }
          final String clusterNames =
              subjectName.substring(CommandExecutorSQLAbstract.CLUSTER_PREFIX.length());
          if (clusterNames.startsWith("[") && clusterNames.endsWith("]")) {
            final Collection<String> clusters = new HashSet<String>(3);
            OStringSerializerHelper.getCollection(clusterNames, 0, clusters);
            for (String cl : clusters) {
              targetClusters.put(cl, cl);
            }
          } else {
            targetClusters.put(clusterNames, alias);
          }

        } else if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.INDEX_PREFIX)) {
          // REGISTER AS INDEX
          targetIndex =
              originalSubjectName.substring(CommandExecutorSQLAbstract.INDEX_PREFIX.length());
        } else if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.METADATA_PREFIX)) {
          // METADATA
          final String metadataTarget =
              subjectName.substring(CommandExecutorSQLAbstract.METADATA_PREFIX.length());
          targetRecords = new ArrayList<YTIdentifiable>();

          if (metadataTarget.equals(CommandExecutorSQLAbstract.METADATA_SCHEMA)) {
            ((ArrayList<YTIdentifiable>) targetRecords)
                .add(
                    new YTRecordId(
                        ODatabaseRecordThreadLocal.instance()
                            .get()
                            .getStorageInfo()
                            .getConfiguration()
                            .getSchemaRecordId()));
          } else if (metadataTarget.equals(CommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
            ((ArrayList<YTIdentifiable>) targetRecords)
                .add(
                    new YTRecordId(
                        ODatabaseRecordThreadLocal.instance()
                            .get()
                            .getStorageInfo()
                            .getConfiguration()
                            .getIndexMgrRecordId()));
          } else {
            throw new YTQueryParsingException("Metadata element not supported: " + metadataTarget);
          }

        } else if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.DICTIONARY_PREFIX)) {
          // DICTIONARY
          final String key =
              originalSubjectName.substring(CommandExecutorSQLAbstract.DICTIONARY_PREFIX.length());
          targetRecords = new ArrayList<YTIdentifiable>();

          final YTIdentifiable value =
              ODatabaseRecordThreadLocal.instance().get().getDictionary().get(key);
          if (value != null) {
            ((List<YTIdentifiable>) targetRecords).add(value);
          }

        } else if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.INDEX_VALUES_PREFIX)) {
          targetIndexValues =
              originalSubjectName.substring(
                  CommandExecutorSQLAbstract.INDEX_VALUES_PREFIX.length());
          targetIndexValuesAsc = true;
        } else if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.INDEX_VALUES_ASC_PREFIX)) {
          targetIndexValues =
              originalSubjectName.substring(
                  CommandExecutorSQLAbstract.INDEX_VALUES_ASC_PREFIX.length());
          targetIndexValuesAsc = true;
        } else if (subjectToMatch.startsWith(
            CommandExecutorSQLAbstract.INDEX_VALUES_DESC_PREFIX)) {
          targetIndexValues =
              originalSubjectName.substring(
                  CommandExecutorSQLAbstract.INDEX_VALUES_DESC_PREFIX.length());
          targetIndexValuesAsc = false;
        } else {
          if (subjectToMatch.startsWith(CommandExecutorSQLAbstract.CLASS_PREFIX))
          // REGISTER AS CLASS
          {
            subjectName = subjectName.substring(CommandExecutorSQLAbstract.CLASS_PREFIX.length());
          }

          // REGISTER AS CLASS
          if (targetClasses == null) {
            targetClasses = new HashMap<String, String>();
          }

          final YTClass cls =
              ODatabaseRecordThreadLocal.instance()
                  .get()
                  .getMetadata()
                  .getImmutableSchemaSnapshot()
                  .getClass(subjectName);
          if (cls == null) {
            throw new YTCommandExecutionException(
                "Class '"
                    + subjectName
                    + "' was not found in database '"
                    + ODatabaseRecordThreadLocal.instance().get().getName()
                    + "'");
          }

          targetClasses.put(cls.getName(), alias);
        }
      }
    }

    return !parserIsEnded();
  }
}
