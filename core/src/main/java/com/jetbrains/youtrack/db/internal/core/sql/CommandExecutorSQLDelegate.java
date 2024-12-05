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

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandExecutor;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.YTCommandExecutorNotFoundException;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import java.util.Map;
import java.util.Set;

/**
 * SQL UPDATE command.
 */
public class CommandExecutorSQLDelegate extends CommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  protected CommandExecutor delegate;

  @SuppressWarnings("unchecked")
  public CommandExecutorSQLDelegate parse(final CommandRequest iCommand) {
    if (iCommand instanceof CommandRequestText textRequest) {
      final String text = textRequest.getText();
      if (text == null) {
        throw new IllegalArgumentException("Command text is null");
      }

      final String textUpperCase = SQLPredicate.upperCase(text);

      delegate = OSQLEngine.getInstance().getCommand(textUpperCase);
      if (delegate == null) {
        throw new YTCommandExecutorNotFoundException(
            "Cannot find a command executor for the command request: " + iCommand);
      }

      delegate.setContext(context);
      delegate.setLimit(iCommand.getLimit());
      delegate.parse(iCommand);
      delegate.setProgressListener(progressListener);
      if (delegate.getFetchPlan() != null) {
        textRequest.setFetchPlan(delegate.getFetchPlan());
      }

    } else {
      throw new YTCommandExecutionException(
          "Cannot find a command executor for the command request: " + iCommand);
    }
    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return delegate.getDistributedTimeout();
  }

  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    return delegate.execute(iArgs, querySession);
  }

  @Override
  public CommandContext getContext() {
    return delegate.getContext();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public String getSyntax() {
    return delegate.getSyntax();
  }

  @Override
  public String getFetchPlan() {
    return delegate.getFetchPlan();
  }

  @Override
  public boolean isIdempotent() {
    return delegate.isIdempotent();
  }

  public CommandExecutor getDelegate() {
    return delegate;
  }

  @Override
  public boolean isCacheable() {
    return delegate.isCacheable();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    if (delegate instanceof OCommandDistributedReplicateRequest) {
      return ((OCommandDistributedReplicateRequest) delegate).getQuorumType();
    }
    return QUORUM_TYPE.ALL;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return delegate.getInvolvedClusters();
  }
}
