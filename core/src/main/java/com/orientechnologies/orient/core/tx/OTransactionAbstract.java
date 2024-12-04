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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import javax.annotation.Nonnull;

public abstract class OTransactionAbstract implements OTransaction {

  @Nonnull
  protected YTDatabaseSessionInternal database;
  protected TXSTATUS status = TXSTATUS.INVALID;

  /**
   * Indicates the record deleted in a transaction.
   *
   * @see #getRecord(YTRID)
   */
  public static final YTRecordAbstract DELETED_RECORD = new YTRecordBytes();

  protected OTransactionAbstract(@Nonnull final YTDatabaseSessionInternal iDatabase) {
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
  public final YTDatabaseSessionInternal getDatabase() {
    return database;
  }

  public abstract void internalRollback();

  public void setDatabase(@Nonnull YTDatabaseSessionInternal database) {
    this.database = database;
  }
}
