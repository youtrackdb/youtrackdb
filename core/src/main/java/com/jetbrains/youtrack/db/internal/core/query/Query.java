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
package com.jetbrains.youtrack.db.internal.core.query;

import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.List;

public interface Query<T extends Object> extends CommandRequest {

  /**
   * Executes the query without limit about the result set. The limit will be bound to the maximum
   * allowed.
   *
   * @return List of records if any record matches the query constraints, otherwise an empty List.
   */
  List<T> run(DatabaseSessionInternal session, Object... iArgs);

  /**
   * Returns the first occurrence found if any
   *
   * @return Record if found, otherwise null
   */
  T runFirst(DatabaseSessionInternal database, Object... iArgs);

  void reset();
}
