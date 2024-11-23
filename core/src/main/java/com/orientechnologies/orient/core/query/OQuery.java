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
package com.orientechnologies.orient.core.query;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import java.util.List;

public interface OQuery<T extends Object> extends OCommandRequest {

  /**
   * Executes the query without limit about the result set. The limit will be bound to the maximum
   * allowed.
   *
   * @return List of records if any record matches the query constraints, otherwise an empty List.
   */
  List<T> run(Object... iArgs);

  /**
   * Returns the first occurrence found if any
   *
   * @return Record if found, otherwise null
   */
  T runFirst(ODatabaseSessionInternal database, Object... iArgs);

  void reset();
}
