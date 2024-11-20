/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/**
 * Listener Interface for all the events of the Database instances.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabaseListener {

  @Deprecated
  void onCreate(final ODatabaseSession iDatabase);

  @Deprecated
  void onDelete(final ODatabaseSession iDatabase);

  @Deprecated
  void onOpen(final ODatabaseSession iDatabase);

  void onBeforeTxBegin(final ODatabaseSession iDatabase);

  void onBeforeTxRollback(final ODatabaseSession iDatabase);

  void onAfterTxRollback(final ODatabaseSession iDatabase);

  void onBeforeTxCommit(final ODatabaseSession iDatabase);

  void onAfterTxCommit(final ODatabaseSession iDatabase);

  void onClose(final ODatabaseSession iDatabase);

  @Deprecated
  void onBeforeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor);

  @Deprecated
  void onAfterCommand(
      final OCommandRequestText iCommand, final OCommandExecutor executor, Object result);

  default void onCreateClass(ODatabaseSession iDatabase, OClass iClass) {}

  default void onDropClass(ODatabaseSession iDatabase, OClass iClass) {}

  default void onCreateView(ODatabaseSession database, OView view) {}

  default void onDropView(ODatabaseSession database, OView view) {}

  default void onCommandStart(ODatabaseSession database, OResultSet resultSet) {}

  default void onCommandEnd(ODatabaseSession database, OResultSet resultSet) {}

  /**
   * Callback to decide if repair the database upon corruption.
   *
   * @param iDatabase        Target database
   * @param iReason          Reason of corruption
   * @param iWhatWillbeFixed TODO
   * @return true if repair must be done, otherwise false
   */
  @Deprecated
  default boolean onCorruptionRepairDatabase(
      final ODatabaseSession iDatabase, final String iReason, String iWhatWillbeFixed) {
    return false;
  }
}
