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
package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Expose the api for extract the internal details needed by the storage for perform the transaction
 * commit
 */
public interface TransactionInternal extends FrontendTransaction {

  /**
   * Extract all the record operations for the current transaction
   *
   * @return the record operations, the collection should not be modified.
   */
  Collection<RecordOperation> getRecordOperations();

  /**
   * Extract all the calculated index operations for the current transaction changes, the key of the
   * map is the index name the value all the changes for the specified index.
   *
   * @return the index changes, the map should not be modified.
   */
  Map<String, FrontendTransactionIndexChanges> getIndexOperations();

  /**
   * Change the status of the transaction.
   *
   * @param iStatus
   */
  void setStatus(final FrontendTransaction.TXSTATUS iStatus);

  /**
   * Access to the database of the transaction
   *
   * @return
   */
  DatabaseSessionInternal getDatabaseSession();

  /**
   * Notify the transaction for the rid change, the changed will be tracked inside the transaction
   * and used for remapping links.
   *
   * @param oldRID the id old value.
   * @param rid    the id new value.
   */
  void updateIdentityAfterCommit(RecordId oldRID, RecordId rid);

  /**
   * Extract a single change from a specified record id.
   *
   * @param currentRid the record id for the change.
   * @return the change or null if there is no change for the specified rid
   */
  RecordOperation getRecordEntry(RID currentRid);

  void setSession(DatabaseSessionInternal session);

  @Nullable
  default byte[] getMetadata() {
    return null;
  }

  void setMetadataHolder(FrontendTransacationMetadataHolder metadata);

  default void storageBegun() {
  }

  Iterator<byte[]> getSerializedOperations();
}
