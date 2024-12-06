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
package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.command.CommandExecutor;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;

/**
 * Listener Interface for all the events of the Database instances.
 */
public interface DatabaseListener {

  @Deprecated
  void onCreate(final DatabaseSession iDatabase);

  @Deprecated
  void onDelete(final DatabaseSession iDatabase);

  @Deprecated
  void onOpen(final DatabaseSession iDatabase);

  void onBeforeTxBegin(final DatabaseSession iDatabase);

  void onBeforeTxRollback(final DatabaseSession iDatabase);

  void onAfterTxRollback(final DatabaseSession iDatabase);

  void onBeforeTxCommit(final DatabaseSession iDatabase);

  void onAfterTxCommit(final DatabaseSession iDatabase);

  void onClose(final DatabaseSession iDatabase);

  @Deprecated
  void onBeforeCommand(final CommandRequestText iCommand, final CommandExecutor executor);

  @Deprecated
  void onAfterCommand(
      final CommandRequestText iCommand, final CommandExecutor executor, Object result);

  default void onCreateClass(DatabaseSession iDatabase, SchemaClass iClass) {
  }

  default void onDropClass(DatabaseSession iDatabase, SchemaClass iClass) {
  }

  default void onCreateView(DatabaseSession database, SchemaView view) {
  }

  default void onDropView(DatabaseSession database, SchemaView view) {
  }

  default void onCommandStart(DatabaseSession database, ResultSet resultSet) {
  }

  default void onCommandEnd(DatabaseSession database, ResultSet resultSet) {
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
      final DatabaseSession iDatabase, final String iReason, String iWhatWillbeFixed) {
    return false;
  }
}
