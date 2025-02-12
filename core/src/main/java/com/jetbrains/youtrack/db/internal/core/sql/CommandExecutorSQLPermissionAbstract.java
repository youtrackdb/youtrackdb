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

import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 */
public abstract class CommandExecutorSQLPermissionAbstract extends CommandExecutorSQLAbstract {

  protected static final String KEYWORD_ON = "ON";
  protected int privilege;
  protected String resource;
  protected Role role;

  protected void parsePrivilege(final StringBuilder word, final int oldPos) {
    final var privilegeName = word.toString();

    if ("CREATE".equals(privilegeName)) {
      privilege = Role.PERMISSION_CREATE;
    } else if ("READ".equals(privilegeName)) {
      privilege = Role.PERMISSION_READ;
    } else if ("UPDATE".equals(privilegeName)) {
      privilege = Role.PERMISSION_UPDATE;
    } else if ("DELETE".equals(privilegeName)) {
      privilege = Role.PERMISSION_DELETE;
    } else if ("EXECUTE".equals(privilegeName)) {
      privilege = Role.PERMISSION_EXECUTE;
    } else if ("ALL".equals(privilegeName)) {
      privilege = Role.PERMISSION_ALL;
    } else if ("NONE".equals(privilegeName)) {
      privilege = Role.PERMISSION_NONE;
    } else {
      throw new CommandSQLParsingException(
          "Unrecognized privilege '" + privilegeName + "'", parserText, oldPos);
    }
  }
}
