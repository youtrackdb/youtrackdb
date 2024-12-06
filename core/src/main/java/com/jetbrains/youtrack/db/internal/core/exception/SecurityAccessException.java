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
package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

public class SecurityAccessException extends SecurityException implements HighLevelException {

  private static final long serialVersionUID = -8486291378415776372L;
  private String databaseName;

  public SecurityAccessException(SecurityAccessException exception) {
    super(exception);

    this.databaseName = exception.databaseName;
  }

  public SecurityAccessException(final String iDatabasename, final String message) {
    super(message);
    databaseName = iDatabasename;
  }

  public SecurityAccessException(final String message) {
    super(message);
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
