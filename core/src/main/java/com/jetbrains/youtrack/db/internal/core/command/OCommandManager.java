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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandExecutorFunction;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandExecutorScript;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandFunction;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandScript;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLDelegate;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLLiveSelect;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLResultsetDelegate;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQLResultset;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLNonBlockingQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class OCommandManager {

  private final Map<String, Class<? extends CommandRequest>> commandRequesters =
      new HashMap<String, Class<? extends CommandRequest>>();
  private final Map<Class<? extends CommandRequest>, OCallable<Void, CommandRequest>>
      configCallbacks =
      new HashMap<Class<? extends CommandRequest>, OCallable<Void, CommandRequest>>();
  private final Map<Class<? extends CommandRequest>, Class<? extends CommandExecutor>>
      commandReqExecMap =
      new HashMap<Class<? extends CommandRequest>, Class<? extends CommandExecutor>>();
  private final Map<String, OScriptExecutor> scriptExecutors = new HashMap<>();

  public OCommandManager() {
    registerScriptExecutor("sql", new OSqlScriptExecutor());
    registerScriptExecutor("script", new OSqlScriptExecutor());
    registerRequester("sql", CommandSQL.class);
    registerRequester("script", CommandScript.class);

    registerExecutor(OSQLAsynchQuery.class, CommandExecutorSQLDelegate.class);
    registerExecutor(OSQLSynchQuery.class, CommandExecutorSQLDelegate.class);
    registerExecutor(OSQLNonBlockingQuery.class, CommandExecutorSQLDelegate.class);
    registerExecutor(LiveQuery.class, CommandExecutorSQLLiveSelect.class);
    registerExecutor(CommandSQL.class, CommandExecutorSQLDelegate.class);
    registerExecutor(CommandSQLResultset.class, CommandExecutorSQLResultsetDelegate.class);
    registerExecutor(CommandScript.class, CommandExecutorScript.class);
    registerExecutor(CommandFunction.class, CommandExecutorFunction.class);
  }

  public OCommandManager registerRequester(
      final String iType, final Class<? extends CommandRequest> iRequest) {
    commandRequesters.put(iType, iRequest);
    return this;
  }

  public OScriptExecutor getScriptExecutor(String language) {
    if (language == null) {
      throw new IllegalArgumentException("Invalid script languange: null");
    }
    OScriptExecutor scriptExecutor = this.scriptExecutors.get(language);
    if (scriptExecutor == null) {
      scriptExecutor = this.scriptExecutors.get(language.toLowerCase(Locale.ENGLISH));
    }
    if (scriptExecutor == null) {
      throw new IllegalArgumentException(
          "Cannot find a script executor requester for language: " + language);
    }

    return scriptExecutor;
  }

  public CommandRequest getRequester(final String iType) {
    final Class<? extends CommandRequest> reqClass = commandRequesters.get(iType);

    if (reqClass == null) {
      throw new IllegalArgumentException("Cannot find a command requester for type: " + iType);
    }

    try {
      return reqClass.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot create the command requester of class " + reqClass + " for type: " + iType, e);
    }
  }

  public OCommandManager registerExecutor(
      final Class<? extends CommandRequest> iRequest,
      final Class<? extends CommandExecutor> iExecutor,
      final OCallable<Void, CommandRequest> iConfigCallback) {
    registerExecutor(iRequest, iExecutor);
    configCallbacks.put(iRequest, iConfigCallback);
    return this;
  }

  public void registerScriptExecutor(String language, OScriptExecutor executor) {
    this.scriptExecutors.put(language, executor);
  }

  public Map<String, OScriptExecutor> getScriptExecutors() {
    return scriptExecutors;
  }

  public OCommandManager registerExecutor(
      final Class<? extends CommandRequest> iRequest,
      final Class<? extends CommandExecutor> iExecutor) {
    commandReqExecMap.put(iRequest, iExecutor);
    return this;
  }

  public OCommandManager unregisterExecutor(final Class<? extends CommandRequest> iRequest) {
    commandReqExecMap.remove(iRequest);
    configCallbacks.remove(iRequest);
    return this;
  }

  public CommandExecutor getExecutor(CommandRequestInternal iCommand) {
    final Class<? extends CommandExecutor> executorClass =
        commandReqExecMap.get(iCommand.getClass());

    if (executorClass == null) {
      throw new YTCommandExecutorNotFoundException(
          "Cannot find a command executor for the command request: " + iCommand);
    }

    try {
      final CommandExecutor exec = executorClass.newInstance();

      final OCallable<Void, CommandRequest> callback = configCallbacks.get(iCommand.getClass());
      if (callback != null) {
        callback.call(iCommand);
      }

      return exec;

    } catch (Exception e) {
      throw YTException.wrapException(
          new YTCommandExecutionException(
              "Cannot create the command executor of class "
                  + executorClass
                  + " for the command request: "
                  + iCommand),
          e);
    }
  }

  public void close(String iDatabaseName) {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.close(iDatabaseName);
    }
  }

  public void closeAll() {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.closeAll();
    }
  }
}
