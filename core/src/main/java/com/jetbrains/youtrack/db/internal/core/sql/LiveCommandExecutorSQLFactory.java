/*
 *
 *  *  Copyright YouTrackDB
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

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Live Query command operator executor factory.
 */
public class LiveCommandExecutorSQLFactory implements CommandExecutorSQLFactory {

  private static Map<String, Class<? extends CommandExecutorSQLAbstract>> COMMANDS =
      new HashMap<String, Class<? extends CommandExecutorSQLAbstract>>();

  static {
    init();
  }

  public static void init() {
    if (COMMANDS.size() == 0) {
      synchronized (LiveCommandExecutorSQLFactory.class) {
        if (COMMANDS.size() == 0) {
          final Map<String, Class<? extends CommandExecutorSQLAbstract>> commands =
              new HashMap<String, Class<? extends CommandExecutorSQLAbstract>>();
          commands.put(
              CommandExecutorSQLLiveSelect.KEYWORD_LIVE_SELECT,
              CommandExecutorSQLLiveSelect.class);
          commands.put(
              CommandExecutorSQLLiveUnsubscribe.KEYWORD_LIVE_UNSUBSCRIBE,
              CommandExecutorSQLLiveUnsubscribe.class);

          COMMANDS = Collections.unmodifiableMap(commands);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public Set<String> getCommandNames() {
    return COMMANDS.keySet();
  }

  /**
   * {@inheritDoc}
   */
  public CommandExecutorSQLAbstract createCommand(final String name)
      throws CommandExecutionException {
    final Class<? extends CommandExecutorSQLAbstract> clazz = COMMANDS.get(name);

    if (clazz == null) {
      throw new CommandExecutionException("Unknowned command name :" + name);
    }

    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw BaseException.wrapException(
          new CommandExecutionException(
              "Error in creation of command "
                  + name
                  + "(). Probably there is not an empty constructor or the constructor generates"
                  + " errors"),
          e);
    }
  }
}
