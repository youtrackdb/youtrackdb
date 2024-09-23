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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;

public interface OTransaction {

  enum TXSTATUS {
    INVALID,
    BEGUN,
    COMMITTING,
    ROLLBACKING,
    COMPLETED,
    ROLLED_BACK
  }

  void begin();

  void commit();

  void commit(boolean force);

  void rollback();

  void rollback(boolean force, int commitLevelDiff);

  ODatabaseDocument getDatabase();

  @Deprecated
  void clearRecordEntries();

  ORecord loadRecord(ORID iRid, ORecordAbstract iRecord, String iFetchPlan, boolean ignoreCache);

  boolean exists(ORID rid);

  ORecord reloadRecord(
      ORID iRid, ORecordAbstract iRecord, String iFetchPlan, boolean ignoreCache, boolean force);

  TXSTATUS getStatus();

  @Deprecated
  Iterable<? extends ORecordOperation> getCurrentRecordEntries();

  Iterable<? extends ORecordOperation> getRecordOperations();

  List<ORecordOperation> getNewRecordEntriesByClass(OClass iClass, boolean iPolymorphic);

  List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds);

  ORecordOperation getRecordEntry(ORID rid);

  List<String> getInvolvedIndexes();

  ODocument getIndexChanges();

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
  void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid);

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
  ORecord saveRecord(ORecordAbstract record, String clusterName);

  /**
   * Deletes the given record in this transaction.
   *
   * @param record the record to delete.
   */
  void deleteRecord(ORecordAbstract record);

  /**
   * Resolves a record with the given RID in the context of this transaction.
   *
   * @param rid the record RID.
   * @return the resolved record, or {@code null} if no record is found, or
   * {@link OTransactionAbstract#DELETED_RECORD} if the record was deleted in this transaction.
   */
  ORecordAbstract getRecord(ORID rid);

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
      OIdentifiable value);

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
  default int getClientTransactionId() {
    return getId();
  }

  int getId();
}
