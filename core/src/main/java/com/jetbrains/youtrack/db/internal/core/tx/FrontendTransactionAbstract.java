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
package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import javax.annotation.Nonnull;

public abstract class FrontendTransactionAbstract implements FrontendTransaction {

  @Nonnull
  protected DatabaseSessionInternal database;
  protected TXSTATUS status = TXSTATUS.INVALID;

  /**
   * Indicates the record deleted in a transaction.
   *
   * @see #getRecord(RID)
   */
  public static final RecordAbstract DELETED_RECORD = new RecordBytes();

  protected FrontendTransactionAbstract(@Nonnull final DatabaseSessionInternal iDatabase) {
    database = iDatabase;
  }

  public boolean isActive() {
    return status != TXSTATUS.INVALID
        && status != TXSTATUS.COMPLETED
        && status != TXSTATUS.ROLLED_BACK;
  }

  public TXSTATUS getStatus() {
    return status;
  }

  @Nonnull
  public final DatabaseSessionInternal getDatabase() {
    return database;
  }

  public abstract void internalRollback();

  public void setDatabase(@Nonnull DatabaseSessionInternal database) {
    this.database = database;
  }
}
