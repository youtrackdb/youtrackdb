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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class OTransactionOptimistic extends OTransactionRealAbstract {

  private static final AtomicInteger txSerial = new AtomicInteger();
  protected boolean changed = true;
  private boolean alreadyCleared = false;
  private boolean usingLog = true;
  protected int txStartCounter;
  private boolean sentToServer = false;

  private final boolean unloadCachedRecords;

  public OTransactionOptimistic(final ODatabaseDocumentInternal iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
    this.unloadCachedRecords = true;
  }

  public OTransactionOptimistic(
      final ODatabaseDocumentInternal iDatabase, boolean unloadCachedRecords) {
    super(iDatabase, txSerial.incrementAndGet());
    this.unloadCachedRecords = unloadCachedRecords;
  }

  public void begin() {
    if (txStartCounter < 0) {
      throw new OTransactionException("Invalid value of TX counter: " + txStartCounter);
    }

    if (txStartCounter == 0) {
      status = TXSTATUS.BEGUN;
      database.getLocalCache().clear();
    } else {
      if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
        throw new ORollbackException(
            "Impossible to start a new transaction because the current was rolled back");
      }
    }

    txStartCounter++;
  }

  public void commit() {
    commit(false);
  }

  /**
   * The transaction is reentrant. If {@code begin()} has been called several times, the actual
   * commit happens only after the same amount of {@code commit()} calls
   *
   * @param force commit transaction even
   */
  @Override
  public void commit(final boolean force) {
    checkTransactionValid();
    if (txStartCounter < 0) {
      throw new OStorageException("Invalid value of tx counter: " + txStartCounter);
    }
    if (force) {
      txStartCounter = 0;
    } else {
      txStartCounter--;
    }

    if (txStartCounter == 0) {
      doCommit();
    } else {
      if (txStartCounter < 0) {
        throw new OTransactionException(
            "Transaction was committed more times than it was started.");
      }
    }
  }

  @Override
  public int amountOfNestedTxs() {
    return txStartCounter;
  }

  public void rollback() {
    rollback(false, -1);
  }

  public void internalRollback() {
    status = TXSTATUS.ROLLBACKING;

    invalidateChangesInCache();

    close();
    status = TXSTATUS.ROLLED_BACK;
  }

  private void invalidateChangesInCache() {
    for (final ORecordOperation v : allEntries.values()) {
      final ORecord rec = v.getRecord();
      ORecordInternal.unsetDirty(rec);
      rec.unload();
    }

    if (unloadCachedRecords) {
      database.getLocalCache().unloadRecords();
    }

    database.getLocalCache().clear();
  }

  @Override
  public boolean isUnloadCachedRecords() {
    return unloadCachedRecords;
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    if (txStartCounter < 0) {
      throw new OStorageException("Invalid value of TX counter");
    }
    checkTransactionValid();

    txStartCounter += commitLevelDiff;
    status = TXSTATUS.ROLLBACKING;

    if (!force && txStartCounter > 0) {
      return;
    }

    if (database.isRemote()) {
      final OStorage storage = database.getStorage();
      ((OStorageProxy) storage).rollback(OTransactionOptimistic.this);
    }

    internalRollback();
  }

  public ORecord loadRecord(
      final ORID rid,
      final ORecordAbstract iRecord,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean loadTombstone,
      final LOCKING_STRATEGY lockingStrategy) {
    return loadRecord(rid, iRecord, fetchPlan, ignoreCache, true, loadTombstone, lockingStrategy);
  }

  public ORecord loadRecord(
      final ORID rid,
      final ORecordAbstract iRecord,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean iUpdateCache,
      final boolean loadTombstone,
      final LOCKING_STRATEGY lockingStrategy) {
    checkTransactionValid();

    if (iRecord != null) {
      iRecord.incrementLoading();
    }
    try {
      final ORecordAbstract txRecord = getRecord(rid);
      if (txRecord == OTransactionAbstract.DELETED_RECORD) {
        // DELETED IN TX
        return null;
      }

      if (txRecord != null) {
        if (iRecord != null && txRecord != iRecord) {
          iRecord.convertToProxyRecord(txRecord);
        }

        return txRecord;
      }

      if (rid.isTemporary()) {
        return null;
      }

      // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
      return database.<ORecordAbstract>executeReadRecord(
          (ORecordId) rid,
          iRecord,
          -1,
          fetchPlan,
          ignoreCache,
          loadTombstone,
          lockingStrategy,
          null);
    } finally {
      if (iRecord != null) {
        iRecord.decrementLoading();
      }
    }
  }

  @Override
  public boolean exists(ORID rid) {
    checkTransactionValid();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      return false;
    }

    if (txRecord != null) {
      return true;
    }

    return database.executeExists(rid);
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(
      ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    checkTransactionValid();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      throw new ORecordNotFoundException(rid);
    }

    if (txRecord != null) {
      if (txRecord.getVersion() > recordVersion) {
        return txRecord;
      } else {
        return null;
      }
    }
    if (rid.isTemporary()) {
      throw new ORecordNotFoundException(rid);
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    return database.<ORecordAbstract>executeReadRecord(
        (ORecordId) rid,
        null,
        recordVersion,
        fetchPlan,
        ignoreCache,
        false,
        LOCKING_STRATEGY.NONE,
        null);
  }

  @Override
  public ORecord reloadRecord(
      ORID rid,
      ORecordAbstract passedRecord,
      String fetchPlan,
      boolean ignoreCache,
      boolean force) {
    checkTransactionValid();

    if (passedRecord != null) {
      passedRecord.incrementLoading();
    }
    try {

      final ORecordAbstract txRecord = getRecord(rid);
      if (txRecord == OTransactionAbstract.DELETED_RECORD) {
        // DELETED IN TX
        return null;
      }

      if (txRecord != null) {
        if (passedRecord != null && txRecord != passedRecord) {
          passedRecord.convertToProxyRecord(txRecord);
        }
        return txRecord;
      }

      if (rid.isTemporary()) {
        return null;
      }

      // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
      final ORecordAbstract record;
      try {
        record =
            database.executeReadRecord(
                (ORecordId) rid,
                passedRecord,
                -1,
                fetchPlan,
                ignoreCache,
                false,
                OStorage.LOCKING_STRATEGY.NONE,
                null);
      } catch (final ORecordNotFoundException ignore) {
        return null;
      }

      return record;
    } finally {
      if (passedRecord != null) {
        passedRecord.decrementLoading();
      }
    }
  }

  @Override
  public ORecord loadRecord(
      ORID rid, ORecordAbstract record, String fetchPlan, boolean ignoreCache) {
    return loadRecord(rid, record, fetchPlan, ignoreCache, false, OStorage.LOCKING_STRATEGY.NONE);
  }

  public void deleteRecord(final ORecordAbstract iRecord) {
    try {
      var records = ORecordInternal.getDirtyManager(iRecord).getUpdateRecords();
      final var newRecords = ORecordInternal.getDirtyManager(iRecord).getNewRecords();
      var recordsMap = new HashMap<>(16);

      if (records != null) {
        for (var rec : records) {
          rec = rec.getRecord();
          var prev = recordsMap.put(rec.getIdentity(), rec);

          if (prev != null && prev != rec) {
            var db = getDatabase();
            throw new IllegalStateException(
                "Database :"
                    + db.getName()
                    + " .For record "
                    + rec
                    + " second instance of record  "
                    + prev
                    + " was registered in dirty manager, such case may lead to data corruption");
          }

          saveRecord(rec, null);
        }
      }

      if (newRecords != null) {
        for (var rec : newRecords) {
          rec = rec.getRecord();
          var prev = recordsMap.put(rec.getIdentity(), rec);
          if (prev != null && prev != rec) {
            var db = getDatabase();
            throw new IllegalStateException(
                "Database :"
                    + db.getName()
                    + " .For record "
                    + rec
                    + " second instance of record  "
                    + prev
                    + " was registered in dirty manager, such case may lead to data corruption");
          }
          saveRecord(rec, null);
        }
      }

      addRecord(iRecord, ORecordOperation.DELETED, null);
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  public ORecord saveRecord(ORecordAbstract passedRecord, final String clusterName) {
    try {
      if (passedRecord == null) {
        return null;
      }
      if (passedRecord.isUnloaded()) {
        return passedRecord;
      }
      // fetch primary record if the record is a proxy record.
      passedRecord = passedRecord.getRecord();

      var recordsMap = new HashMap<>(16);
      recordsMap.put(passedRecord.getIdentity(), passedRecord);

      boolean originalSaved = false;
      final ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(passedRecord);
      do {
        final var newRecord = dirtyManager.getNewRecords();
        final var updatedRecord = dirtyManager.getUpdateRecords();
        dirtyManager.clear();
        if (newRecord != null) {
          for (ORecordAbstract rec : newRecord) {
            rec = rec.getRecord();

            var prev = recordsMap.put(rec.getIdentity(), rec);
            if (prev != null && prev != rec) {
              var db = getDatabase();
              throw new IllegalStateException(
                  "Database :"
                      + db.getName()
                      + " .For record "
                      + rec
                      + " second instance of record  "
                      + prev
                      + " was registered in dirty manager, such case may lead to data corruption");
            }

            if (rec instanceof ODocument) {
              ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
            }
            if (rec == passedRecord) {
              addRecord(rec, ORecordOperation.CREATED, clusterName);
              originalSaved = true;
            } else {
              addRecord(rec, ORecordOperation.CREATED, database.getClusterName(rec));
            }
          }
        }
        if (updatedRecord != null) {
          for (var rec : updatedRecord) {
            rec = rec.getRecord();

            var prev = recordsMap.put(rec.getIdentity(), rec);
            if (prev != null && prev != rec) {
              var db = getDatabase();
              throw new IllegalStateException(
                  "Database :"
                      + db.getName()
                      + " .For record "
                      + rec
                      + " second instance of record  "
                      + prev
                      + " was registered in dirty manager, such case may lead to data corruption");
            }

            if (rec instanceof ODocument) {
              ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
            }
            if (rec == passedRecord) {
              final byte operation =
                  passedRecord.getIdentity().isValid()
                      ? ORecordOperation.UPDATED
                      : ORecordOperation.CREATED;

              addRecord(rec, operation, clusterName);
              originalSaved = true;
            } else {
              addRecord(rec, ORecordOperation.UPDATED, database.getClusterName(rec));
            }
          }
        }
      } while (dirtyManager.getNewRecords() != null || dirtyManager.getUpdateRecords() != null);

      if (!originalSaved && passedRecord.isDirty()) {
        final byte operation =
            passedRecord.getIdentity().isValid()
                ? ORecordOperation.UPDATED
                : ORecordOperation.CREATED;
        addRecord(passedRecord, operation, clusterName);
      }
      return passedRecord;
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  @Override
  public String toString() {
    return "OTransactionOptimistic [id="
        + id
        + ", status="
        + status
        + ", recEntries="
        + allEntries.size()
        + ", idxEntries="
        + indexEntries.size()
        + ']';
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public void setUsingLog(final boolean useLog) {
    this.usingLog = useLog;
  }

  public void setStatus(final TXSTATUS iStatus) {
    status = iStatus;
  }

  public ORecordOperation addRecord(ORecordAbstract iRecord, byte iStatus, String iClusterName) {
    changed = true;
    checkTransactionValid();

    if (iClusterName == null) {
      iClusterName = database.getClusterNameById(iRecord.getIdentity().getClusterId());
    }

    try {
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      ORecordOperation txEntry = getRecordEntry(rid);
      if (iStatus == ORecordOperation.CREATED && txEntry != null) {
        iStatus = ORecordOperation.UPDATED;
      }
      switch (iStatus) {
        case ORecordOperation.CREATED:
          {
            OIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (ORecordAbstract) res;
            }
          }
          break;
        case ORecordOperation.UPDATED:
          {
            OIdentifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (ORecordAbstract) res;
            }
          }
          break;
        case ORecordOperation.DELETED:
          database.beforeDeleteOperations(iRecord, iClusterName);
          break;
      }

      try {
        if (!rid.isValid()) {
          ORecordInternal.onBeforeIdentityChanged(iRecord);
          database.assignAndCheckCluster(iRecord, iClusterName);

          rid.setClusterPosition(newObjectCounter--);

          ORecordInternal.onAfterIdentityChanged(iRecord);
        }
        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(iRecord, iStatus);
            allEntries.put(rid.copy(), txEntry);
          }
        } else {
          // UPDATE PREVIOUS STATUS
          txEntry.record = iRecord;

          switch (txEntry.type) {
            case ORecordOperation.UPDATED:
              if (iStatus == ORecordOperation.DELETED) {
                txEntry.type = ORecordOperation.DELETED;
              }
              break;
            case ORecordOperation.DELETED:
              break;
            case ORecordOperation.CREATED:
              if (iStatus == ORecordOperation.DELETED) {
                allEntries.remove(rid);
                // txEntry.type = ORecordOperation.DELETED;
              }
              break;
          }
        }

        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.afterCreateOperations(iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.afterUpdateOperations(iRecord);
            break;
          case ORecordOperation.DELETED:
            database.afterDeleteOperations(iRecord);
            break;
        }

        // RESET TRACKING
        if (iRecord instanceof ODocument && ((ODocument) iRecord).isTrackingChanges()) {
          ODocumentInternal.clearTrackData(((ODocument) iRecord));
        }
        return txEntry;
      } catch (final Exception e) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(TYPE.CREATE_FAILED, iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(TYPE.UPDATE_FAILED, iRecord);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(TYPE.DELETE_FAILED, iRecord);
            break;
        }
        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      switch (iStatus) {
        case ORecordOperation.CREATED:
          database.callbackHooks(TYPE.FINALIZE_CREATION, iRecord);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(TYPE.FINALIZE_UPDATE, iRecord);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(TYPE.FINALIZE_DELETION, iRecord);
          break;
      }
    }
  }

  private void doCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
      if (status == TXSTATUS.ROLLBACKING) {
        internalRollback();
      }

      throw new ORollbackException(
          "Given transaction was rolled back, and thus cannot be committed.");
    }

    try {
      status = TXSTATUS.COMMITTING;

      if (sentToServer || !allEntries.isEmpty() || !indexEntries.isEmpty()) {
        database.internalCommit(this);
      }
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }

    close();
    status = TXSTATUS.COMPLETED;
  }

  @Override
  public void addIndexEntry(
      OIndex delegate,
      String iIndexName,
      OTransactionIndexChanges.OPERATION iOperation,
      Object key,
      OIdentifiable iValue,
      boolean clientTrackOnly) {
    changed = true;
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue, clientTrackOnly);
  }

  public void resetChangesTracking() {
    alreadyCleared = true;
    changed = false;
  }

  @Override
  public void close() {
    final OLocalRecordCache dbCache = database.getLocalCache();
    for (ORecordOperation txEntry : allEntries.values()) {
      var record = txEntry.getRecord();

      if (!record.isUnloaded()) {
        if (record instanceof ODocument document) {
          ODocumentInternal.clearTransactionTrackData(document);
        }

        ORecordInternal.unsetDirty(record);
        record.unload();
      }
    }

    if (unloadCachedRecords) {
      dbCache.unloadRecords();
    }

    dbCache.clear();

    super.close();
  }

  public boolean isChanged() {
    return changed;
  }

  public boolean isAlreadyCleared() {
    return alreadyCleared;
  }

  public Set<ORID> getLockedRecords() {
    if (getNoTxLocks() != null) {
      final Set<ORID> rids = new HashSet<>(getNoTxLocks().keySet());
      rids.addAll(locks.keySet());
      return rids;
    } else {
      return locks.keySet();
    }
  }

  public void setSentToServer(boolean sentToServer) {
    this.sentToServer = sentToServer;
  }

  public void fill(final Iterator<ORecordOperation> operations) {
    while (operations.hasNext()) {
      ORecordOperation change = operations.next();
      allEntries.put(change.getRID(), change);
      resolveTracking(change);
    }
  }

  protected void resolveTracking(final ORecordOperation change) {
    if (!(change.getRecord() instanceof ODocument rec)) {
      return;
    }

    switch (change.getType()) {
      case ORecordOperation.CREATED:
        {
          final ODocument doc = (ODocument) change.getRecord();
          OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.CREATED, database);
          final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            OClassIndexManager.processIndexOnCreate(database, rec);
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().createdFunction(doc);
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceCreated(database, doc);
            }
            if (clazz.isScheduler()) {
              database.getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
            }
          }
        }
        break;
      case ORecordOperation.UPDATED:
        {
          final OIdentifiable updateRecord = change.getRecord();
          if (updateRecord instanceof ODocument updateDoc) {
            OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
            OLiveQueryHookV2.addOp(updateDoc, ORecordOperation.UPDATED, database);
            final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
            if (clazz != null) {
              OClassIndexManager.processIndexOnUpdate(database, updateDoc);
              if (clazz.isFunction()) {
                database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
              }
            }
          }
        }
        break;
      case ORecordOperation.DELETED:
        {
          final ODocument doc = (ODocument) change.getRecord();
          final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            OClassIndexManager.processIndexOnDelete(database, rec);
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().droppedFunction(doc);
              database
                  .getSharedContext()
                  .getOrientDB()
                  .getScriptManager()
                  .close(database.getName());
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceDropped(database, doc);
            }
            if (clazz.isScheduler()) {
              final String eventName = doc.field(OScheduledEvent.PROP_NAME);
              database.getSharedContext().getScheduler().removeEventInternal(eventName);
            }
          }
          OLiveQueryHook.addOp(doc, ORecordOperation.DELETED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.DELETED, database);
        }
        break;
      default:
        break;
    }
  }

  protected int getTxStartCounter() {
    return txStartCounter;
  }
}
