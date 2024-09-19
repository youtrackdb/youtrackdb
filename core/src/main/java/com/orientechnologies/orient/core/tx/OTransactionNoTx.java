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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * No operation transaction.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OTransactionNoTx extends OTransactionAbstract {

  public OTransactionNoTx(final ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
  }

  public void begin() {
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

  @Deprecated
  public ORecord loadRecord(
      final ORID iRid,
      final ORecordAbstract iRecord,
      final String iFetchPlan,
      final boolean ignoreCache,
      final boolean loadTombstone) {
    if (iRid.isNew()) {
      return null;
    }

    if (iRecord != null) {
      iRecord.incrementLoading();
    }
    try {
      return database.executeReadRecord(
          (ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, loadTombstone, null);
    } finally {
      if (iRecord != null) {
        iRecord.decrementLoading();
      }
    }
  }

  @Deprecated
  public ORecord loadRecord(
      final ORID iRid,
      final ORecordAbstract iRecord,
      final String iFetchPlan,
      final boolean ignoreCache,
      final boolean iUpdateCache,
      final boolean loadTombstone) {
    if (iRid.isNew()) {
      return null;
    }

    if (iRecord != null) {
      iRecord.incrementLoading();
    }
    try {
      return database.executeReadRecord(
          (ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, loadTombstone, null);
    } finally {
      if (iRecord != null) {
        iRecord.decrementLoading();
      }
    }
  }

  public ORecord loadRecord(
      final ORID iRid,
      final ORecordAbstract iRecord,
      final String iFetchPlan,
      final boolean ignoreCache) {
    if (iRid.isNew()) {
      return null;
    }

    if (iRecord != null) {
      iRecord.incrementLoading();
    }
    try {
      return database.executeReadRecord(
          (ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, false, null);
    } finally {
      if (iRecord != null) {
        iRecord.decrementLoading();
      }
    }
  }

  @Override
  public boolean exists(ORID rid) {
    if (rid.isNew()) {
      return false;
    }

    return database.executeExists(rid);
  }

  @Override
  public ORecord reloadRecord(
      ORID rid, ORecordAbstract record, String fetchPlan, boolean ignoreCache, boolean force) {
    if (rid.isNew()) {
      return null;
    }

    if (record != null) {
      record.incrementLoading();
    }
    try {
      final ORecord loadedRecord =
          database.executeReadRecord(
              (ORecordId) rid, record, -1, fetchPlan, ignoreCache, false, null);

      if (force) {
        return loadedRecord;
      } else {
        if (loadedRecord == null) {
          return record;
        }

        return loadedRecord;
      }
    } finally {
      if (record != null) {
        record.decrementLoading();
      }
    }
  }

  public ORecord saveRecord(final ORecordAbstract iRecord, final String iClusterName) {
    throw new ODatabaseException("Cannot save record in no tx mode");
  }

  /**
   * Deletes the record.
   */
  public void deleteRecord(final ORecordAbstract iRecord) {
    throw new ODatabaseException("Cannot delete record in no tx mode");
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return Collections.emptyList();
  }

  public Collection<ORecordOperation> getRecordOperations() {
    return Collections.emptyList();
  }

  public List<ORecordOperation> getNewRecordEntriesByClass(
      final OClass iClass, final boolean iPolymorphic) {
    return Collections.emptyList();
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    return Collections.emptyList();
  }

  public void clearRecordEntries() {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  public ORecordAbstract getRecord(final ORID rid) {
    return null;
  }

  public ORecordOperation getRecordEntry(final ORID rid) {
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

  public ODocument getIndexChanges() {
    return null;
  }

  public void addIndexEntry(
      final OIndex delegate,
      final String indexName,
      final OPERATION status,
      final Object key,
      final OIdentifiable value) {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  public void clearIndexEntries() {
    throw new UnsupportedOperationException("Operation not supported in no tx mode");
  }

  @Override
  public void close() {}

  public OTransactionIndexChanges getIndexChanges(final String iName) {
    return null;
  }

  public int getId() {
    return 0;
  }

  public List<String> getInvolvedIndexes() {
    return Collections.emptyList();
  }

  public void updateIdentityAfterCommit(ORID oldRid, ORID newRid) {
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
  public void internalRollback() {}
}
