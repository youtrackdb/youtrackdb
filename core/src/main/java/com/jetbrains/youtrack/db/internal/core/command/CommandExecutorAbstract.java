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
package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.listener.OProgressListener;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.OExecutionThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandInterruptedException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract implementation of Executor Command interface.
 */
@SuppressWarnings("unchecked")
public abstract class CommandExecutorAbstract extends BaseParser implements CommandExecutor {

  protected OProgressListener progressListener;
  protected int limit = -1;
  protected Map<Object, Object> parameters;
  protected CommandContext context;

  public static YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public CommandExecutorAbstract init(final CommandRequestText iRequest) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.COMMAND, ORole.PERMISSION_READ);
    parserText = iRequest.getText().trim();
    parserTextUpperCase = SQLPredicate.upperCase(parserText);
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [text=" + parserText + "]";
  }

  public OProgressListener getProgressListener() {
    return progressListener;
  }

  public <RET extends CommandExecutor> RET setProgressListener(
      final OProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  public String getUndoCommand() {
    return null;
  }

  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(GlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT);
  }

  public int getLimit() {
    return limit;
  }

  public <RET extends CommandExecutor> RET setLimit(final int iLimit) {
    this.limit = iLimit;
    return (RET) this;
  }

  public Map<Object, Object> getParameters() {
    return parameters;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  public CommandContext getContext() {
    return context;
  }

  public void setContext(final CommandContext iContext) {
    context = iContext;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return Collections.EMPTY_SET;
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_READ;
  }

  public boolean involveSchema() {
    return false;
  }

  protected boolean checkInterruption() {
    return checkInterruption(this.context);
  }

  public static boolean checkInterruption(final CommandContext iContext) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new YTCommandInterruptedException("The command has been interrupted");
    }

    return iContext == null || iContext.checkTimeout();
  }

  public OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT
  getDistributedResultManagement() {
    return OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
  }

  @Override
  public boolean isLocalExecution() {
    return false;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  public Object mergeResults(final Map<String, Object> results) throws Exception {

    if (results.isEmpty()) {
      return null;
    }

    Object aggregatedResult = null;

    for (Map.Entry<String, Object> entry : results.entrySet()) {
      final String nodeName = entry.getKey();
      final Object nodeResult = entry.getValue();

      if (nodeResult instanceof Collection) {
        if (aggregatedResult == null) {
          aggregatedResult = new ArrayList();
        }

        ((List) aggregatedResult).addAll((Collection<?>) nodeResult);

      } else if (nodeResult instanceof Exception)

      // RECEIVED EXCEPTION
      {
        throw (Exception) nodeResult;
      } else if (nodeResult instanceof YTIdentifiable) {
        if (aggregatedResult == null) {
          aggregatedResult = new ArrayList();
        }

        ((List) aggregatedResult).add(nodeResult);

      } else if (nodeResult instanceof Number) {
        if (aggregatedResult == null) {
          aggregatedResult = nodeResult;
        } else {
          OMultiValue.add(aggregatedResult, nodeResult);
        }
      }
    }

    return aggregatedResult;
  }

  public boolean isDistributedExecutingOnLocalNodeFirst() {
    return true;
  }
}
