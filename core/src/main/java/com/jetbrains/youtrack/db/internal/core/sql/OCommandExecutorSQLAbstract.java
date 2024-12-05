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
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext.TIMEOUT_STRATEGY;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandExecutorAbstract;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestAbstract;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClassImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatementCache;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL abstract Command Executor implementation.
 */
public abstract class OCommandExecutorSQLAbstract extends OCommandExecutorAbstract {

  public static final String KEYWORD_FROM = "FROM";
  public static final String KEYWORD_LET = "LET";
  public static final String KEYWORD_WHERE = "WHERE";
  public static final String KEYWORD_LIMIT = "LIMIT";
  public static final String KEYWORD_SKIP = "SKIP";
  public static final String KEYWORD_OFFSET = "OFFSET";
  public static final String KEYWORD_TIMEOUT = "TIMEOUT";
  public static final String KEYWORD_RETURN = "RETURN";
  public static final String KEYWORD_KEY = "key";
  public static final String KEYWORD_RID = "rid";
  public static final String CLUSTER_PREFIX = "CLUSTER:";
  public static final String CLASS_PREFIX = "CLASS:";
  public static final String INDEX_PREFIX = "INDEX:";
  public static final String KEYWORD_UNSAFE = "UNSAFE";

  public static final String INDEX_VALUES_PREFIX = "INDEXVALUES:";
  public static final String INDEX_VALUES_ASC_PREFIX = "INDEXVALUESASC:";
  public static final String INDEX_VALUES_DESC_PREFIX = "INDEXVALUESDESC:";

  public static final String DICTIONARY_PREFIX = "DICTIONARY:";
  public static final String METADATA_PREFIX = "METADATA:";
  public static final String METADATA_SCHEMA = "SCHEMA";
  public static final String METADATA_INDEXMGR = "INDEXMANAGER";
  public static final String METADATA_STORAGE = "STORAGE";
  public static final String METADATA_DATABASE = "DATABASE";
  public static final String METADATA_DISTRIBUTED = "DISTRIBUTED";

  public static final String DEFAULT_PARAM_USER = "$user";

  protected long timeoutMs = GlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();
  protected TIMEOUT_STRATEGY timeoutStrategy = TIMEOUT_STRATEGY.EXCEPTION;
  protected OStatement preParsedStatement;

  /**
   * The command is replicated
   *
   * @return
   */
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE
  getDistributedExecutionMode() {
    return OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.REPLICATE;
  }

  public boolean isIdempotent() {
    return false;
  }

  protected void throwSyntaxErrorException(final String iText) {
    throw new YTCommandSQLParsingException(
        iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }

  protected void throwParsingException(final String iText) {
    throw new YTCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());
  }

  protected void throwParsingException(final String iText, Exception e) {
    throw YTException.wrapException(
        new YTCommandSQLParsingException(iText, parserText, parserGetPreviousPosition()), e);
  }

  /**
   * Parses the timeout keyword if found.
   */
  protected boolean parseTimeout(final String w) throws YTCommandSQLParsingException {
    if (!w.equals(KEYWORD_TIMEOUT)) {
      return false;
    }

    String word = parserNextWord(true);

    try {
      timeoutMs = Long.parseLong(word);
    } catch (NumberFormatException ignore) {
      throwParsingException(
          "Invalid "
              + KEYWORD_TIMEOUT
              + " value set to '"
              + word
              + "' but it should be a valid long. Example: "
              + KEYWORD_TIMEOUT
              + " 3000");
    }

    if (timeoutMs < 0) {
      throwParsingException(
          "Invalid "
              + KEYWORD_TIMEOUT
              + ": value set minor than ZERO. Example: "
              + KEYWORD_TIMEOUT
              + " 10000");
    }

    word = parserNextWord(true);

    if (word != null) {
      if (word.equals(TIMEOUT_STRATEGY.EXCEPTION.toString())) {
        timeoutStrategy = TIMEOUT_STRATEGY.EXCEPTION;
      } else if (word.equals(TIMEOUT_STRATEGY.RETURN.toString())) {
        timeoutStrategy = TIMEOUT_STRATEGY.RETURN;
      } else {
        parserGoBack();
      }
    }

    return true;
  }

  protected Set<String> getInvolvedClustersOfClasses(final Collection<String> iClassNames) {
    final var db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    for (String clazz : iClassNames) {
      final YTClass cls = db.getMetadata().getImmutableSchemaSnapshot().getClass(clazz);
      if (cls != null) {
        for (int clId : cls.getPolymorphicClusterIds()) {
          // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
          if (clId > -1 && checkClusterAccess(db, db.getClusterNameById(clId))) {
            clusters.add(db.getClusterNameById(clId).toLowerCase(Locale.ENGLISH));
          }
        }
      }
    }

    return clusters;
  }

  protected Set<String> getInvolvedClustersOfClusters(final Collection<String> iClusterNames) {
    var db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    for (String cluster : iClusterNames) {
      final String c = cluster.toLowerCase(Locale.ENGLISH);
      // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
      if (checkClusterAccess(db, c)) {
        clusters.add(c);
      }
    }

    return clusters;
  }

  protected Set<String> getInvolvedClustersOfIndex(final String iIndexName) {
    final YTDatabaseSessionInternal db = getDatabase();

    final Set<String> clusters = new HashSet<String>();

    final OMetadataInternal metadata = db.getMetadata();
    final OIndex idx = metadata.getIndexManagerInternal().getIndex(db, iIndexName);
    if (idx != null && idx.getDefinition() != null) {
      final String clazz = idx.getDefinition().getClassName();

      if (clazz != null) {
        final YTClass cls = metadata.getImmutableSchemaSnapshot().getClass(clazz);
        if (cls != null) {
          for (int clId : cls.getClusterIds()) {
            final String clName = db.getClusterNameById(clId);
            if (clName != null) {
              clusters.add(clName.toLowerCase(Locale.ENGLISH));
            }
          }
        }
      }
    }

    return clusters;
  }

  protected boolean checkClusterAccess(final YTDatabaseSessionInternal db,
      final String iClusterName) {
    return db.getUser() == null
        || db.getUser()
        .checkIfAllowed(db,
            ORule.ResourceGeneric.CLUSTER, iClusterName, getSecurityOperationType())
        != null;
  }

  protected void bindDefaultContextVariables() {
    if (context != null) {
      if (getDatabase() != null && getDatabase().getUser() != null) {
        context.setVariable(DEFAULT_PARAM_USER, getDatabase().getUser().getIdentity(getDatabase()));
      }
    }
  }

  protected String preParse(final String queryText, final OCommandRequest iRequest) {
    final boolean strict = getDatabase().getStorageInfo().getConfiguration().isStrictSql();

    if (strict) {
      try {
        final OStatement result = OStatementCache.get(queryText, getDatabase());
        preParsedStatement = result;

        if (iRequest instanceof OCommandRequestAbstract) {
          final Map<Object, Object> params = ((OCommandRequestAbstract) iRequest).getParameters();
          StringBuilder builder = new StringBuilder();
          result.toString(params, builder);
          return builder.toString();
        }
        return result.toString();
      } catch (YTCommandSQLParsingException sqlx) {
        throw sqlx;
      } catch (Exception e) {
        throwParsingException("Error parsing query: \n" + queryText + "\n" + e.getMessage(), e);
      }
    }
    return queryText;
  }

  protected String decodeClassName(String s) {
    return YTClassImpl.decodeClassName(s);
  }
}
