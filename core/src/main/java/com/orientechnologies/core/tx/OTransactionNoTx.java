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
package com.orientechnologies.core.tx;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.ORecordOperation;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTNoTxRecordReadException;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.tx.OTransactionIndexChanges.OPERATION;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * No operation transaction.
 */
public class OTransactionNoTx extends OTransactionAbstract {

  private static final String NON_TX_WARNING_READ_MESSAGE =
      "Read operation performed in no tx mode. "
          + "Such behavior can lead to inconsistent state of the database. "
          + "Please consider using transaction or set "
          + YTGlobalConfiguration.NON_TX_READS_WARNING_MODE.getKey()
          + " configuration parameter to "
          + NonTxReadMode.SILENT.name()
          + " to avoid this warning, or to "
          + NonTxReadMode.EXCEPTION.name()
          + " to throw an exception for such cases.";
  private static final String NON_TX_EXCEPTION_READ_MESSAGE =
      "Read operation performed in no tx mode. "
          + "Such behavior can lead to inconsistent state of the database."
          + " Please consider using transaction or set "
          + YTGlobalConfiguration.NON_TX_READS_WARNING_MODE.getKey()
          + " configuration parameter to "
          + NonTxReadMode.SILENT.name()
          + " to avoid this warning, or to "
          + NonTxReadMode.WARN.name()
          + " to show a warning for such cases.";

  private final NonTxReadMode nonTxReadMode;

  public OTransactionNoTx(final YTDatabaseSessionInternal database) {
    super(database);

    this.nonTxReadMode = database.getNonTxReadMode();
    assert this.nonTxReadMode != null;
  }

  public int begin() {
    throw new UnsupportedOperationException("Begin is not supported in no tx mode");
  }

  public void commit() {
    throw new UnsupportedOperationException("Commit is not supported in no tx mode");
  }

  @Override
  public int getEntryCount() {
    return 0;
  }

  @Override
  public void commit(boolean force) {
    throw new UnsupportedOperationException("Commit is not supported in no tx mode");
  }

  public void rollback() {
    throw new UnsupportedOperationException("Rollback is not supported in no tx mode");
  }

  public @Nonnull YTRecord loadRecord(final YTRID rid) {
    checkNonTXReads();
    if (rid.isNew()) {
      throw new YTRecordNotFoundException(rid);
    }

    return database.executeReadRecord((YTRecordId) rid);
  }

  private void checkNonTXReads() {
    if (nonTxReadMode == NonTxReadMode.WARN) {
      OLogManager.instance().warn(this, NON_TX_WARNING_READ_MESSAGE);
    } else if (nonTxReadMode == NonTxReadMode.EXCEPTION) {
      throw new YTNoTxRecordReadException(NON_TX_EXCEPTION_READ_MESSAGE);
    }
  }

  @Override
  public boolean exists(YTRID rid) {
    checkNonTXReads();
    if (rid.isNew()) {
      return false;
    }

    return database.executeExists(rid);
  }

  public YTRecord saveRecord(final YTRecordAbstract iRecord, final String iClusterName) {
    throw new YTDatabaseException("Cannot save record in no tx mode");
  }

  /**
   * Deletes the record.
   */
  public void deleteRecord(final YTRecordAbstract iRecord) {
    throw new YTDatabaseException("Cannot delete record in no tx mode");
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return Collections.emptyList();
  }

  public Collection<ORecordOperation> getRecordOperations() {
    return Collections.emptyList();
  }

  public List<ORecordOperation> getNewRecordEntriesByClass(
      final YTClass iClass, final boolean iPolymorphic) {
    return Collections.emptyList();
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    return Collections.emptyList();
  }

  public void clearRecordEntries() {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  public YTRecordAbstract getRecord(final YTRID rid) {
    checkNonTXReads();

    return null;
  }

  public ORecordOperation getRecordEntry(final YTRID rid) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  @Override
  public void setCustomData(String iName, Object iValue) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  @Override
  public Object getCustomData(String iName) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  public YTEntityImpl getIndexChanges() {
    return null;
  }

  public void addIndexEntry(
      final OIndex delegate,
      final String indexName,
      final OPERATION status,
      final Object key,
      final YTIdentifiable value) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  public void clearIndexEntries() {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  @Override
  public void close() {
  }

  public OTransactionIndexChanges getIndexChanges(final String iName) {
    return null;
  }

  public long getId() {
    return 0;
  }

  public List<String> getInvolvedIndexes() {
    return Collections.emptyList();
  }

  public void updateIdentityAfterCommit(YTRID oldRid, YTRID newRid) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  @Override
  public int amountOfNestedTxs() {
    return 0;
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    throw new UnsupportedOperationException("Rollback is not supported in no tx mode");
  }

  @Override
  public OTransactionIndexChanges getIndexChangesInternal(String indexName) {
    return null;
  }

  @Override
  public void internalRollback() {
  }

  public enum NonTxReadMode {
    WARN,
    EXCEPTION,
    SILENT
  }
}
