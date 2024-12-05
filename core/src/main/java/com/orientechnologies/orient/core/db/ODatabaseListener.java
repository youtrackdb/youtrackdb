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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTView;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;

/**
 * Listener Interface for all the events of the Database instances.
 */
public interface ODatabaseListener {

  @Deprecated
  void onCreate(final YTDatabaseSession iDatabase);

  @Deprecated
  void onDelete(final YTDatabaseSession iDatabase);

  @Deprecated
  void onOpen(final YTDatabaseSession iDatabase);

  void onBeforeTxBegin(final YTDatabaseSession iDatabase);

  void onBeforeTxRollback(final YTDatabaseSession iDatabase);

  void onAfterTxRollback(final YTDatabaseSession iDatabase);

  void onBeforeTxCommit(final YTDatabaseSession iDatabase);

  void onAfterTxCommit(final YTDatabaseSession iDatabase);

  void onClose(final YTDatabaseSession iDatabase);

  @Deprecated
  void onBeforeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor);

  @Deprecated
  void onAfterCommand(
      final OCommandRequestText iCommand, final OCommandExecutor executor, Object result);

  default void onCreateClass(YTDatabaseSession iDatabase, YTClass iClass) {
  }

  default void onDropClass(YTDatabaseSession iDatabase, YTClass iClass) {
  }

  default void onCreateView(YTDatabaseSession database, YTView view) {
  }

  default void onDropView(YTDatabaseSession database, YTView view) {
  }

  default void onCommandStart(YTDatabaseSession database, YTResultSet resultSet) {
  }

  default void onCommandEnd(YTDatabaseSession database, YTResultSet resultSet) {
  }

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
      final YTDatabaseSession iDatabase, final String iReason, String iWhatWillbeFixed) {
    return false;
  }
}
