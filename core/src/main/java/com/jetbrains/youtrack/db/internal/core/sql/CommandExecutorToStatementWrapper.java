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

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandExecutor;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.StatementCache;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper for OPrifileStorageStatement command (for compatibility with the old executor
 * architecture, this component should be removed)
 */
public class CommandExecutorToStatementWrapper implements CommandExecutor {

  protected SQLAsynchQuery<EntityImpl> request;
  private CommandContext context;
  private ProgressListener progressListener;

  protected SQLStatement statement;

  @SuppressWarnings("unchecked")
  @Override
  public CommandExecutorToStatementWrapper parse(CommandRequest iCommand) {
    final CommandRequestText textRequest = (CommandRequestText) iCommand;
    if (iCommand instanceof SQLAsynchQuery) {
      request = (SQLAsynchQuery<EntityImpl>) iCommand;
    } else {
      // BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
      request = new SQLSynchQuery<EntityImpl>(textRequest.getText());
      if (textRequest.getResultListener() != null) {
        request.setResultListener(textRequest.getResultListener());
      }
    }
    String queryText = textRequest.getText();
    statement = StatementCache.get(queryText, getDatabase());
    return this;
  }

  public static DatabaseSessionInternal getDatabase() {
    return DatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public Object execute(Map<Object, Object> iArgs, DatabaseSessionInternal querySession) {
    return statement.execute(request, context, this.progressListener);
  }

  @Override
  public <RET extends CommandExecutor> RET setProgressListener(
      ProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  @Override
  public <RET extends CommandExecutor> RET setLimit(int iLimit) {
    return (RET) this;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  @Override
  public Map<Object, Object> getParameters() {
    return null;
  }

  @Override
  public CommandContext getContext() {
    return this.context;
  }

  @Override
  public void setContext(CommandContext context) {
    this.context = context;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return Collections.EMPTY_SET;
  }

  @Override
  public int getSecurityOperationType() {
    return Role.PERMISSION_READ;
  }

  @Override
  public boolean involveSchema() {
    return false;
  }

  @Override
  public String getSyntax() {
    return "PROFILE STORAGE [ON | OFF]";
  }

  @Override
  public boolean isLocalExecution() {
    return true;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  @Override
  public long getDistributedTimeout() {
    return 0;
  }

  @Override
  public Object mergeResults(Map<String, Object> results) throws Exception {
    return null;
  }
}
