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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import javax.annotation.Nonnull;

public interface OTransaction {

  enum TXSTATUS {
    INVALID,
    BEGUN,
    COMMITTING,
    ROLLBACKING,
    COMPLETED,
    ROLLED_BACK
  }

  int begin();

  void commit();

  void commit(boolean force);

  void rollback();

  void rollback(boolean force, int commitLevelDiff);

  YTDatabaseSession getDatabase();

  @Deprecated
  void clearRecordEntries();

  @Nonnull
  Record loadRecord(YTRID rid) throws YTRecordNotFoundException;

  boolean exists(YTRID rid);

  TXSTATUS getStatus();

  @Deprecated
  Iterable<? extends ORecordOperation> getCurrentRecordEntries();

  Iterable<? extends ORecordOperation> getRecordOperations();

  List<ORecordOperation> getNewRecordEntriesByClass(YTClass iClass, boolean iPolymorphic);

  List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds);

  ORecordOperation getRecordEntry(YTRID rid);

  List<String> getInvolvedIndexes();

  EntityImpl getIndexChanges();

  @Deprecated
  void clearIndexEntries();

  void close();

  /**
   * When commit in transaction is performed all new records will change their identity, but index
   * values will contain stale links, to fix them given method will be called for each entry. This
   * update local transaction maps too.
   *
   * @param oldRid Record identity before commit.
   * @param newRid Record identity after commit.
   */
  void updateIdentityAfterCommit(final YTRID oldRid, final YTRID newRid);

  int amountOfNestedTxs();

  int getEntryCount();

  /**
   * @return {@code true} if this transaction is active, {@code false} otherwise.
   */
  boolean isActive();

  /**
   * Saves the given record in this transaction.
   *
   * @param record      the record to save.
   * @param clusterName record's cluster name.
   * @return the record saved.
   */
  Record saveRecord(RecordAbstract record, String clusterName);

  /**
   * Deletes the given record in this transaction.
   *
   * @param record the record to delete.
   */
  void deleteRecord(RecordAbstract record);

  /**
   * Resolves a record with the given RID in the context of this transaction.
   *
   * @param rid the record RID.
   * @return the resolved record, or {@code null} if no record is found, or
   * {@link OTransactionAbstract#DELETED_RECORD} if the record was deleted in this transaction.
   */
  RecordAbstract getRecord(YTRID rid);

  /**
   * Adds the transactional index entry in this transaction.
   *
   * @param index     the index.
   * @param indexName the index name.
   * @param operation the index operation to register.
   * @param key       the index key.
   * @param value     the index key value.
   */
  void addIndexEntry(
      OIndex index,
      String indexName,
      OTransactionIndexChanges.OPERATION operation,
      Object key,
      YTIdentifiable value);

  /**
   * Obtains the index changes done in the context of this transaction.
   *
   * @param indexName the index name.
   * @return the index changes in question or {@code null} if index is not found.
   */
  OTransactionIndexChanges getIndexChanges(String indexName);

  /**
   * Does the same thing as {@link #getIndexChanges(String)}, but handles remote storages in a
   * special way.
   *
   * @param indexName the index name.
   * @return the index changes in question or {@code null} if index is not found or storage is
   * remote.
   */
  OTransactionIndexChanges getIndexChangesInternal(String indexName);

  /**
   * Obtains the custom value by its name stored in the context of this transaction.
   *
   * @param name the value name.
   * @return the obtained value or {@code null} if no value found.
   */
  Object getCustomData(String name);

  /**
   * Sets the custom value by its name stored in the context of this transaction.
   *
   * @param name  the value name.
   * @param value the value to store.
   */
  void setCustomData(String name, Object value);

  /**
   * @return this transaction ID as seen by the client of this transaction.
   */
  default long getClientTransactionId() {
    return getId();
  }

  long getId();
}
