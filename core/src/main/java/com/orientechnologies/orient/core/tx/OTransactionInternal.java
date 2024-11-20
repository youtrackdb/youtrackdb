/*
 *
 *  *  Copyright 2017-2018 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nullable;

/**
 * Expose the api for extract the internal details needed by the storage for perform the transaction
 * commit
 */
public interface OTransactionInternal extends OTransaction {

  /**
   * Extract all the record operations for the current transaction
   *
   * @return the record operations, the collection should not be modified.
   */
  Collection<ORecordOperation> getRecordOperations();

  /**
   * Extract all the calculated index operations for the current transaction changes, the key of the
   * map is the index name the value all the changes for the specified index.
   *
   * @return the index changes, the map should not be modified.
   */
  Map<String, OTransactionIndexChanges> getIndexOperations();

  /**
   * Change the status of the transaction.
   *
   * @param iStatus
   */
  void setStatus(final OTransaction.TXSTATUS iStatus);

  /**
   * Access to the database of the transaction
   *
   * @return
   */
  ODatabaseSessionInternal getDatabase();

  /**
   * Notify the transaction for the rid change, the changed will be tracked inside the transaction
   * and used for remapping links.
   *
   * @param oldRID the id old value.
   * @param rid    the id new value.
   */
  void updateIdentityAfterCommit(ORID oldRID, ORID rid);

  /**
   * Extract a single change from a specified record id.
   *
   * @param currentRid the record id for the change.
   * @return the change or null if there is no change for the specified rid
   */
  ORecordOperation getRecordEntry(ORID currentRid);

  void setDatabase(ODatabaseSessionInternal database);

  @Nullable
  default byte[] getMetadata() {
    return null;
  }

  void setMetadataHolder(OTxMetadataHolder metadata);

  default void storageBegun() {}

  void prepareSerializedOperations() throws IOException;

  Iterator<byte[]> getSerializedOperations();

  default void fill(Iterator<ORecordOperation> operations) {
    throw new UnsupportedOperationException();
  }

  void resetAllocatedIds();
}
