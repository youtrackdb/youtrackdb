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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ChangeableIdentity;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
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
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OTransactionOptimistic extends OTransactionAbstract implements OTransactionInternal {

  private static final AtomicLong txSerial = new AtomicLong();

  // order of updates is critical during synchronization of remote transactions
  protected LinkedHashMap<ORID, ORID> txGeneratedRealRecordIdMap = new LinkedHashMap<>();
  protected LinkedHashMap<ORID, ORecordOperation> recordOperations = new LinkedHashMap<>();

  protected LinkedHashMap<String, OTransactionIndexChanges> indexEntries = new LinkedHashMap<>();
  protected HashMap<ORID, List<OTransactionRecordIndexOperation>> recordIndexOperations =
      new HashMap<>();

  protected long id;
  protected int newRecordsPositionsGenerator = -2;
  private final HashMap<String, Object> userData = new HashMap<>();

  public int serverBeginCount = 0;

  @Nullable
  private OTxMetadataHolder metadata = null;

  @Nullable
  private List<byte[]> serializedOperations;

  protected boolean changed = true;
  private boolean isAlreadyStartedOnServer = false;
  protected int txStartCounter;
  private boolean sentToServer = false;

  public OTransactionOptimistic(final ODatabaseSessionInternal iDatabase) {
    super(iDatabase);
    this.id = txSerial.incrementAndGet();
  }

  protected OTransactionOptimistic(final ODatabaseSessionInternal iDatabase, long id) {
    super(iDatabase);
    this.id = id;
  }

  public int begin() {
    if (txStartCounter < 0) {
      throw new OTransactionException("Invalid value of TX counter: " + txStartCounter);
    }

    if (txStartCounter == 0) {
      status = TXSTATUS.BEGUN;

      var localCache = database.getLocalCache();
      localCache.unloadNotModifiedRecords();
      localCache.clear();
    } else {
      if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
        throw new ORollbackException(
            "Impossible to start a new transaction because the current was rolled back");
      }
    }

    txStartCounter++;
    return txStartCounter;
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

  public ORecordAbstract getRecord(final ORID rid) {
    final ORecordOperation e = getRecordEntry(rid);
    if (e != null) {
      if (e.type == ORecordOperation.DELETED) {
        return OTransactionAbstract.DELETED_RECORD;
      } else {
        return e.record;
      }
    }
    return null;
  }

  /**
   * Called by class iterator.
   */
  public List<ORecordOperation> getNewRecordEntriesByClass(
      final OClass iClass, final boolean iPolymorphic) {
    final List<ORecordOperation> result = new ArrayList<>();

    if (iClass == null)
    // RETURN ALL THE RECORDS
    {
      for (ORecordOperation entry : recordOperations.values()) {
        if (entry.type == ORecordOperation.CREATED) {
          result.add(entry);
        }
      }
    } else {
      // FILTER RECORDS BY CLASSNAME
      for (ORecordOperation entry : recordOperations.values()) {
        if (entry.type == ORecordOperation.CREATED) {
          if (entry.record != null) {
            if (entry.record instanceof ODocument) {
              if (iPolymorphic) {
                if (iClass.isSuperClassOf(
                    ODocumentInternal.getImmutableSchemaClass(((ODocument) entry.record)))) {
                  result.add(entry);
                }
              } else {
                if (iClass.getName().equals(((ODocument) entry.record).getClassName())) {
                  result.add(entry);
                }
              }
            }
          }
        }
      }
    }

    return result;
  }

  /**
   * Called by cluster iterator.
   */
  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    final List<ORecordOperation> result = new ArrayList<>();

    if (iIds == null)
    // RETURN ALL THE RECORDS
    {
      for (ORecordOperation entry : recordOperations.values()) {
        if (entry.type == ORecordOperation.CREATED) {
          result.add(entry);
        }
      }
    } else
    // FILTER RECORDS BY ID
    {
      for (ORecordOperation entry : recordOperations.values()) {
        for (int id : iIds) {
          if (entry.record != null) {
            if (entry.record.getIdentity().getClusterId() == id
                && entry.type == ORecordOperation.CREATED) {
              result.add(entry);
              break;
            }
          }
        }
      }
    }

    return result;
  }

  public void clearIndexEntries() {
    indexEntries.clear();
    recordIndexOperations.clear();
  }

  public List<String> getInvolvedIndexes() {
    List<String> list = null;
    for (String indexName : indexEntries.keySet()) {
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(indexName);
    }
    return list;
  }

  public ODocument getIndexChanges() {

    final ODocument result = new ODocument().setAllowChainedAccess(false).setTrackingChanges(false);

    for (Entry<String, OTransactionIndexChanges> indexEntry : indexEntries.entrySet()) {
      final ODocument indexDoc = new ODocument().setTrackingChanges(false);
      ODocumentInternal.addOwner(indexDoc, result);

      result.field(indexEntry.getKey(), indexDoc, OType.EMBEDDED);

      if (indexEntry.getValue().cleared) {
        indexDoc.field("clear", Boolean.TRUE);
      }

      final List<ODocument> entries = new ArrayList<>();
      indexDoc.field("entries", entries, OType.EMBEDDEDLIST);

      // STORE INDEX ENTRIES
      for (OTransactionIndexChangesPerKey entry : indexEntry.getValue().changesPerKey.values()) {
        entries.add(serializeIndexChangeEntry(entry, indexDoc));
      }

      indexDoc.field(
          "nullEntries", serializeIndexChangeEntry(indexEntry.getValue().nullKeyChanges, indexDoc));
    }

    indexEntries.clear();

    return result;
  }

  public Map<String, OTransactionIndexChanges> getIndexOperations() {
    return indexEntries;
  }

  public OTransactionIndexChanges getIndexChangesInternal(final String indexName) {
    if (getDatabase().isRemote()) {
      return null;
    }
    return getIndexChanges(indexName);
  }

  public void addIndexEntry(
      final OIndex delegate,
      final String iIndexName,
      final OTransactionIndexChanges.OPERATION iOperation,
      final Object key,
      final OIdentifiable iValue) {
    // index changes are tracked on server in case of client-server deployment
    assert database.getStorage() instanceof OAbstractPaginatedStorage;

    changed = true;

    try {
      OTransactionIndexChanges indexEntry = indexEntries.get(iIndexName);
      if (indexEntry == null) {
        indexEntry = new OTransactionIndexChanges();
        indexEntries.put(iIndexName, indexEntry);
      }

      if (iOperation == OPERATION.CLEAR) {
        indexEntry.setCleared();
      } else {
        OTransactionIndexChangesPerKey changes = indexEntry.getChangesPerKey(key);
        changes.add(iValue, iOperation);

        if (changes.key == key
            && key instanceof ChangeableIdentity changeableIdentity
            && changeableIdentity.canChangeIdentity()) {
          changeableIdentity.addIdentityChangeListener(indexEntry);
        }

        if (iValue == null) {
          return;
        }

        List<OTransactionRecordIndexOperation> transactionIndexOperations =
            recordIndexOperations.get(iValue.getIdentity());

        if (transactionIndexOperations == null) {
          transactionIndexOperations = new ArrayList<>();
          recordIndexOperations.put(iValue.getIdentity().copy(), transactionIndexOperations);
        }

        transactionIndexOperations.add(
            new OTransactionRecordIndexOperation(iIndexName, key, iOperation));
      }
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  /**
   * Buffer sizes index changes to be flushed at commit time.
   */
  public OTransactionIndexChanges getIndexChanges(final String iIndexName) {
    return indexEntries.get(iIndexName);
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
    for (final ORecordOperation v : recordOperations.values()) {
      final ORecordAbstract rec = v.record;
      ORecordInternal.unsetDirty(rec);
      rec.unload();
    }

    var localCache = database.getLocalCache();
    localCache.unloadRecords();
    localCache.clear();
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
  public @Nonnull ORecord loadRecord(ORID rid) {

    checkTransactionValid();

    final ORecordAbstract txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      throw new ORecordNotFoundException(rid);
    }

    if (txRecord != null) {
      return txRecord;
    }

    if (rid.isTemporary()) {
      throw new ORecordNotFoundException(rid);
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    return database.<ORecordAbstract>executeReadRecord((ORecordId) rid);
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
        throw new ODatabaseException(
            "Record "
                + passedRecord
                + " is not bound to session, please call "
                + ODatabaseSession.class.getSimpleName()
                + ".bindToSession(record) before changing it");
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
        + recordOperations.size()
        + ", idxEntries="
        + indexEntries.size()
        + ']';
  }

  public void setStatus(final TXSTATUS iStatus) {
    status = iStatus;
  }

  public void addRecord(ORecordAbstract record, byte status, String clusterName) {
    if (record.isUnloaded()) {
      throw new ODatabaseException(
          "Record "
              + record
              + " is not bound to session, please call "
              + ODatabaseSession.class.getSimpleName()
              + ".bindToSession(record) before changing it");
    }
    changed = true;
    checkTransactionValid();

    if (clusterName == null) {
      clusterName = database.getClusterNameById(record.getIdentity().getClusterId());
    }

    try {
      final ORecordId rid = (ORecordId) record.getIdentity();
      ORecordOperation txEntry = getRecordEntry(rid);

      if (txEntry != null) {
        if (txEntry.record != record) {
          throw new OTransactionException(
              "Found record in transaction with the same RID but different instance");
        }
      }

      if (status == ORecordOperation.CREATED && txEntry != null) {
        status = ORecordOperation.UPDATED;
      }
      switch (status) {
        case ORecordOperation.CREATED: {
          OIdentifiable res = database.beforeCreateOperations(record, clusterName);
          if (res != null) {
            record = (ORecordAbstract) res;
          }
        }
        break;
        case ORecordOperation.UPDATED: {
          OIdentifiable res = database.beforeUpdateOperations(record, clusterName);
          if (res != null) {
            record = (ORecordAbstract) res;
          }
        }
        break;
        case ORecordOperation.DELETED:
          database.beforeDeleteOperations(record, clusterName);
          break;
      }

      try {
        if (!rid.isValid()) {
          ORecordInternal.onBeforeIdentityChanged(record);
          database.assignAndCheckCluster(record, clusterName);

          rid.setClusterPosition(newRecordsPositionsGenerator--);

          ORecordInternal.onAfterIdentityChanged(record);
        }
        if (txEntry == null) {
          if (!(rid.isTemporary() && status != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(record, status);
            recordOperations.put(rid.copy(), txEntry);
          }
        } else {
          // UPDATE PREVIOUS STATUS
          txEntry.record = record;

          switch (txEntry.type) {
            case ORecordOperation.UPDATED:
              if (status == ORecordOperation.DELETED) {
                txEntry.type = ORecordOperation.DELETED;
              }
              break;
            case ORecordOperation.DELETED:
              break;
            case ORecordOperation.CREATED:
              if (status == ORecordOperation.DELETED) {
                recordOperations.remove(rid);
              }
              break;
          }
        }

        switch (status) {
          case ORecordOperation.CREATED:
            database.afterCreateOperations(record);
            break;
          case ORecordOperation.UPDATED:
            database.afterUpdateOperations(record);
            break;
          case ORecordOperation.DELETED:
            database.afterDeleteOperations(record);
            break;
        }

        // RESET TRACKING
        if (record instanceof ODocument && ((ODocument) record).isTrackingChanges()) {
          ODocumentInternal.clearTrackData(((ODocument) record));
        }
      } catch (final Exception e) {
        switch (status) {
          case ORecordOperation.CREATED:
            database.callbackHooks(TYPE.CREATE_FAILED, record);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(TYPE.UPDATE_FAILED, record);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(TYPE.DELETE_FAILED, record);
            break;
        }
        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + record.getIdentity()), e);
      }
    } finally {
      switch (status) {
        case ORecordOperation.CREATED:
          database.callbackHooks(TYPE.FINALIZE_CREATION, record);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(TYPE.FINALIZE_UPDATE, record);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(TYPE.FINALIZE_DELETION, record);
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

      if (sentToServer || !recordOperations.isEmpty() || !indexEntries.isEmpty()) {
        database.internalCommit(this);

        try {
          database.afterCommitOperations();
        } catch (Exception e) {
          OLogManager.instance().error(this,
              "Error during after commit callback invocation", e);
        }
      }

    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }

    close();
    status = TXSTATUS.COMPLETED;
  }

  public void resetChangesTracking() {
    isAlreadyStartedOnServer = true;
    changed = false;
  }

  @Override
  public void close() {
    final OLocalRecordCache dbCache = database.getLocalCache();
    for (ORecordOperation txEntry : recordOperations.values()) {
      var record = txEntry.record;

      if (!record.isUnloaded()) {
        if (record instanceof ODocument document) {
          ODocumentInternal.clearTransactionTrackData(document);
        }

        ORecordInternal.unsetDirty(record);
        record.unload();
      }
    }

    dbCache.unloadRecords();
    dbCache.clear();

    clearUnfinishedChanges();

    status = TXSTATUS.INVALID;
  }

  private void clearUnfinishedChanges() {
    recordOperations.clear();
    indexEntries.clear();
    recordIndexOperations.clear();

    newRecordsPositionsGenerator = -2;

    database.setDefaultTransactionMode();
    userData.clear();
  }

  public void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid) {
    if (oldRid.equals(newRid))
    // NO CHANGE, IGNORE IT
    {
      return;
    }

    // XXX: Identity update may mutate the index keys, so we have to identify and reinsert
    // potentially affected index keys to keep
    // the OTransactionIndexChanges.changesPerKey in a consistent state.

    final List<KeyChangesUpdateRecord> keyRecordsToReinsert = new ArrayList<>();
    final ODatabaseSessionInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    for (Entry<String, OTransactionIndexChanges> entry : indexEntries.entrySet()) {
      final OIndex index = indexManager.getIndex(database, entry.getKey());
      if (index == null) {
        throw new OTransactionException(
            "Cannot find index '" + entry.getValue() + "' while committing transaction");
      }

      final Dependency[] fieldRidDependencies = getIndexFieldRidDependencies(index);
      if (!isIndexMayDependOnRids(fieldRidDependencies)) {
        continue;
      }

      final OTransactionIndexChanges indexChanges = entry.getValue();
      for (final Iterator<OTransactionIndexChangesPerKey> iterator =
          indexChanges.changesPerKey.values().iterator();
          iterator.hasNext(); ) {
        final OTransactionIndexChangesPerKey keyChanges = iterator.next();
        if (isIndexKeyMayDependOnRid(keyChanges.key, oldRid, fieldRidDependencies)) {
          keyRecordsToReinsert.add(new KeyChangesUpdateRecord(keyChanges, indexChanges));
          iterator.remove();

          if (keyChanges.key instanceof ChangeableIdentity changeableIdentity) {
            changeableIdentity.removeIdentityChangeListener(indexChanges);
          }
        }
      }
    }

    // Update the identity.

    final ORecordOperation rec = getRecordEntry(oldRid);
    if (rec != null) {
      txGeneratedRealRecordIdMap.put(newRid.copy(), oldRid.copy());

      if (!rec.record.getIdentity().equals(newRid)) {
        ORecordInternal.onBeforeIdentityChanged(rec.record);

        final ORecordId recordId = (ORecordId) rec.record.getIdentity();
        if (recordId == null) {
          ORecordInternal.setIdentity(rec.record, new ORecordId(newRid));
        } else {
          recordId.setClusterPosition(newRid.getClusterPosition());
          recordId.setClusterId(newRid.getClusterId());
        }

        ORecordInternal.onAfterIdentityChanged(rec.record);
      }
    }

    // Reinsert the potentially affected index keys.

    for (KeyChangesUpdateRecord record : keyRecordsToReinsert) {
      record.indexChanges.changesPerKey.put(record.keyChanges.key, record.keyChanges);
    }

    // Update the indexes.

    ORecordOperation val = getRecordEntry(oldRid);
    final List<OTransactionRecordIndexOperation> transactionIndexOperations =
        recordIndexOperations.get(val != null ? val.getRID() : null);
    if (transactionIndexOperations != null) {
      for (final OTransactionRecordIndexOperation indexOperation : transactionIndexOperations) {
        OTransactionIndexChanges indexEntryChanges = indexEntries.get(indexOperation.index);
        if (indexEntryChanges == null) {
          continue;
        }
        final OTransactionIndexChangesPerKey keyChanges;
        if (indexOperation.key == null) {
          keyChanges = indexEntryChanges.nullKeyChanges;
        } else {
          keyChanges = indexEntryChanges.changesPerKey.get(indexOperation.key);
        }
        if (keyChanges != null) {
          updateChangesIdentity(oldRid, newRid, keyChanges);
        }
      }
    }
  }

  private ODocument serializeIndexChangeEntry(
      OTransactionIndexChangesPerKey entry, final ODocument indexDoc) {
    // SERIALIZE KEY

    ODocument keyContainer = new ODocument();
    keyContainer.setTrackingChanges(false);

    if (entry.key != null) {
      if (entry.key instanceof OCompositeKey) {
        final List<Object> keys = ((OCompositeKey) entry.key).getKeys();

        keyContainer.field("key", keys, OType.EMBEDDEDLIST);
        keyContainer.field("binary", false);
      } else {
        keyContainer.field("key", entry.key);
        keyContainer.field("binary", false);
      }

    } else {
      keyContainer = null;
    }

    final List<ODocument> operations = new ArrayList<>();

    // SERIALIZE VALUES
    if (!entry.isEmpty()) {
      for (OTransactionIndexEntry e : entry.getEntriesAsList()) {

        final ODocument changeDoc = new ODocument().setAllowChainedAccess(false);
        ODocumentInternal.addOwner(changeDoc, indexDoc);

        // SERIALIZE OPERATION
        changeDoc.field("o", e.getOperation().ordinal());

        if (e.getValue() instanceof ORecord && e.getValue().getIdentity().isNew()) {
          final ORecord saved = getRecord(e.getValue().getIdentity());
          if (saved != null) {
            e.setValue(saved);
          } else {
            ((ORecord) e.getValue()).save();
          }
        }

        changeDoc.field("v", e.getValue() != null ? e.getValue().getIdentity() : null);

        operations.add(changeDoc);
      }
    }
    ODocument res = new ODocument();
    res.setTrackingChanges(false);
    ODocumentInternal.addOwner(res, indexDoc);
    return res.setAllowChainedAccess(false)
        .field("k", keyContainer, OType.EMBEDDED)
        .field("ops", operations, OType.EMBEDDEDLIST);
  }

  private void updateChangesIdentity(
      ORID oldRid, ORID newRid, OTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null) {
      return;
    }

    for (final OTransactionIndexEntry indexEntry : changesPerKey.getEntriesAsList()) {
      if (indexEntry.getValue().getIdentity().equals(oldRid)) {
        indexEntry.setValue(newRid);
      }
    }
  }

  @Override
  public void setCustomData(String iName, Object iValue) {
    userData.put(iName, iValue);
  }

  @Override
  public Object getCustomData(String iName) {
    return userData.get(iName);
  }

  private static Dependency[] getIndexFieldRidDependencies(OIndex index) {
    final OIndexDefinition definition = index.getDefinition();

    if (definition == null) { // type for untyped index is still not resolved
      return null;
    }

    final OType[] types = definition.getTypes();
    final Dependency[] dependencies = new Dependency[types.length];

    for (int i = 0; i < types.length; ++i) {
      dependencies[i] = getTypeRidDependency(types[i]);
    }

    return dependencies;
  }

  private static boolean isIndexMayDependOnRids(Dependency[] fieldDependencies) {
    if (fieldDependencies == null) {
      return true;
    }

    for (Dependency dependency : fieldDependencies) {
      switch (dependency) {
        case Unknown:
        case Yes:
          return true;
        case No:
          break; // do nothing
      }
    }

    return false;
  }

  private static boolean isIndexKeyMayDependOnRid(
      Object key, ORID rid, Dependency[] keyDependencies) {
    if (key instanceof OCompositeKey) {
      final List<Object> subKeys = ((OCompositeKey) key).getKeys();
      for (int i = 0; i < subKeys.size(); ++i) {
        if (isIndexKeyMayDependOnRid(
            subKeys.get(i), rid, keyDependencies == null ? null : keyDependencies[i])) {
          return true;
        }
      }
      return false;
    }

    return isIndexKeyMayDependOnRid(key, rid, keyDependencies == null ? null : keyDependencies[0]);
  }

  private static boolean isIndexKeyMayDependOnRid(Object key, ORID rid, Dependency dependency) {
    if (dependency == Dependency.No) {
      return false;
    }

    if (key instanceof OIdentifiable) {
      return key.equals(rid);
    }

    return dependency == Dependency.Unknown || dependency == null;
  }

  private static Dependency getTypeRidDependency(OType type) {
    // fallback to the safest variant, just in case
    return switch (type) {
      case CUSTOM, ANY -> Dependency.Unknown;
      case EMBEDDED, LINK -> Dependency.Yes;
      case LINKLIST, LINKSET, LINKMAP, LINKBAG, EMBEDDEDLIST, EMBEDDEDSET, EMBEDDEDMAP ->
        // under normal conditions, collection field type is already resolved to its
        // component type
          throw new IllegalStateException("Collection field type is not allowed here");
      default -> // all other primitive types which doesn't depend on rids
          Dependency.No;
    };
  }

  private enum Dependency {
    Unknown,
    Yes,
    No
  }

  private static class KeyChangesUpdateRecord {

    final OTransactionIndexChangesPerKey keyChanges;
    final OTransactionIndexChanges indexChanges;

    KeyChangesUpdateRecord(
        OTransactionIndexChangesPerKey keyChanges, OTransactionIndexChanges indexChanges) {
      this.keyChanges = keyChanges;
      this.indexChanges = indexChanges;
    }
  }

  protected void checkTransactionValid() {
    if (status == TXSTATUS.INVALID) {
      throw new OTransactionException(
          "Invalid state of the transaction. The transaction must be begun.");
    }
  }

  public boolean isChanged() {
    return changed;
  }

  public boolean isStartedOnServer() {
    return isAlreadyStartedOnServer;
  }

  public void setSentToServer(boolean sentToServer) {
    this.sentToServer = sentToServer;
  }

  public long getId() {
    return id;
  }

  public void clearRecordEntries() {
  }

  public void restore() {
  }

  @Override
  public int getEntryCount() {
    return recordOperations.size();
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return recordOperations.values();
  }

  public Collection<ORecordOperation> getRecordOperations() {
    return recordOperations.values();
  }

  public ORecordOperation getRecordEntry(ORID ridPar) {
    ORID rid = ridPar;
    ORecordOperation entry;
    do {
      entry = recordOperations.get(rid);
      if (entry == null) {
        rid = txGeneratedRealRecordIdMap.get(rid);
      }
    } while (entry == null && rid != null && !rid.equals(ridPar));
    return entry;
  }

  public Map<ORID, ORID> getTxGeneratedRealRecordIdMap() {
    return txGeneratedRealRecordIdMap;
  }

  @Override
  @Nullable
  public byte[] getMetadata() {
    if (metadata != null) {
      return metadata.metadata();
    }
    return null;
  }

  @Override
  public void storageBegun() {
    if (metadata != null) {
      metadata.notifyMetadataRead();
    }
  }

  @Override
  public void setMetadataHolder(OTxMetadataHolder metadata) {
    this.metadata = metadata;
  }

  @Override
  public void prepareSerializedOperations() throws IOException {
    List<byte[]> operations = new ArrayList<>();
    for (ORecordOperation value : recordOperations.values()) {
      OTransactionDataChange change = new OTransactionDataChange(database, value);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      change.serialize(new DataOutputStream(out));
      operations.add(out.toByteArray());
    }
    this.serializedOperations = operations;
  }

  public Iterator<byte[]> getSerializedOperations() {
    if (serializedOperations != null) {
      return serializedOperations.iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public void resetAllocatedIds() {
    for (Map.Entry<ORID, ORecordOperation> op : recordOperations.entrySet()) {
      if (op.getValue().type == ORecordOperation.CREATED) {
        ORID lastCreateId = op.getValue().getRID().copy();
        ORecordId oldNew =
            new ORecordId(lastCreateId.getClusterId(), op.getKey().getClusterPosition());
        updateIdentityAfterCommit(lastCreateId, oldNew);
        txGeneratedRealRecordIdMap.put(oldNew, op.getKey());
      }
    }
  }

  public void fill(final Iterator<ORecordOperation> operations) {
    while (operations.hasNext()) {
      ORecordOperation change = operations.next();
      recordOperations.put(change.getRID(), change);
      resolveTracking(change);
    }
  }

  protected void resolveTracking(final ORecordOperation change) {
    if (!(change.record instanceof ODocument rec)) {
      return;
    }

    switch (change.type) {
      case ORecordOperation.CREATED: {
        final ODocument doc = (ODocument) change.record;
        OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, database);
        OLiveQueryHookV2.addOp(database, doc, ORecordOperation.CREATED);
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
            database.getMetadata().getScheduler()
                .scheduleEvent(database, new OScheduledEvent(doc, database));
          }
        }
      }
      break;
      case ORecordOperation.UPDATED: {
        final OIdentifiable updateRecord = change.record;
        ODocument updateDoc = (ODocument) updateRecord;
        OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
        OLiveQueryHookV2.addOp(database, updateDoc, ORecordOperation.UPDATED);
        final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
        if (clazz != null) {
          OClassIndexManager.processIndexOnUpdate(database, updateDoc);
          if (clazz.isFunction()) {
            database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
          }
        }
      }
      break;
      case ORecordOperation.DELETED: {
        final ODocument doc = (ODocument) change.record;
        final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
        if (clazz != null) {
          OClassIndexManager.processIndexOnDelete(database, rec);
          if (clazz.isFunction()) {
            database.getSharedContext().getFunctionLibrary().droppedFunction(doc);
            database
                .getSharedContext()
                .getOxygenDB()
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
        OLiveQueryHookV2.addOp(database, doc, ORecordOperation.DELETED);
      }
      break;
      default:
        break;
    }
  }

  public int getTxStartCounter() {
    return txStartCounter;
  }
}
