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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.TransactionException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.RecordHook.TYPE;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableIdentity;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.ClassIndexManager;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.function.FunctionLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.schedule.SchedulerImpl;
import com.jetbrains.youtrack.db.internal.core.storage.StorageProxy;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class FrontendTransactionOptimistic extends FrontendTransactionAbstract implements
    TransactionInternal {

  private static final AtomicLong txSerial = new AtomicLong();

  // order of updates is critical during synchronization of remote transactions
  protected LinkedHashMap<RecordId, RecordId> generatedOriginalRecordIdMap = new LinkedHashMap<>();
  protected LinkedHashMap<RecordId, RecordOperation> recordOperations = new LinkedHashMap<>();

  protected LinkedHashMap<String, FrontendTransactionIndexChanges> indexEntries = new LinkedHashMap<>();
  protected HashMap<RID, List<FrontendTransactionRecordIndexOperation>> recordIndexOperations =
      new HashMap<>();

  protected long id;
  protected int newRecordsPositionsGenerator = -2;
  private final HashMap<String, Object> userData = new HashMap<>();

  @Nullable
  private FrontendTransacationMetadataHolder metadata = null;

  @Nullable
  private List<byte[]> serializedOperations;

  protected boolean changed = true;
  private boolean isAlreadyStartedOnServer = false;
  protected int txStartCounter;
  private boolean sentToServer = false;

  public FrontendTransactionOptimistic(final DatabaseSessionInternal iDatabase) {
    super(iDatabase);
    this.id = txSerial.incrementAndGet();
  }

  protected FrontendTransactionOptimistic(final DatabaseSessionInternal iDatabase, long id) {
    super(iDatabase);
    this.id = id;
  }

  public int begin() {
    if (txStartCounter < 0) {
      throw new TransactionException(session, "Invalid value of TX counter: " + txStartCounter);
    }

    if (txStartCounter == 0) {
      status = TXSTATUS.BEGUN;

      var localCache = session.getLocalCache();
      localCache.unloadNotModifiedRecords();
      localCache.clear();
    } else {
      if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
        throw new RollbackException(
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
      throw new TransactionException(session.getDatabaseName(),
          "Invalid value of tx counter: " + txStartCounter);
    }
    if (force) {
      preProcessRecordsAndExecuteCallCallbacks();
      txStartCounter = 0;
    } else {
      if (txStartCounter == 1) {
        preProcessRecordsAndExecuteCallCallbacks();
      }
      txStartCounter--;
    }

    if (txStartCounter == 0) {
      doCommit();
    } else {
      if (txStartCounter < 0) {
        throw new TransactionException(session,
            "Transaction was committed more times than it was started.");
      }
    }
  }

  public RecordAbstract getRecord(final RID rid) {
    final var e = getRecordEntry(rid);
    if (e != null) {
      if (e.type == RecordOperation.DELETED) {
        return FrontendTransactionAbstract.DELETED_RECORD;
      } else {
        assert e.record.getSession() == session;
        return e.record;
      }
    }
    return null;
  }

  /**
   * Called by class iterator.
   */
  public List<RecordOperation> getNewRecordEntriesByClass(
      final SchemaClass iClass, final boolean iPolymorphic) {
    final List<RecordOperation> result = new ArrayList<>();

    if (iClass == null)
    // RETURN ALL THE RECORDS
    {
      for (var entry : recordOperations.values()) {
        if (entry.type == RecordOperation.CREATED) {
          result.add(entry);
        }
      }
    } else {
      // FILTER RECORDS BY CLASSNAME
      for (var entry : recordOperations.values()) {
        if (entry.type == RecordOperation.CREATED) {
          if (entry.record != null) {
            if (entry.record instanceof EntityImpl entity) {
              if (iPolymorphic) {
                var cls = entity.getImmutableSchemaClass(session);
                if (iClass.isSuperClassOf(session, cls)) {
                  result.add(entry);
                }
              } else {
                if (iClass.getName(session)
                    .equals(((EntityImpl) entry.record).getSchemaClassName())) {
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
  public List<RecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    final List<RecordOperation> result = new ArrayList<>();

    if (iIds == null)
    // RETURN ALL THE RECORDS
    {
      for (var entry : recordOperations.values()) {
        if (entry.type == RecordOperation.CREATED) {
          result.add(entry);
        }
      }
    } else
    // FILTER RECORDS BY ID
    {
      for (var entry : recordOperations.values()) {
        for (var id : iIds) {
          if (entry.record != null) {
            if (entry.record.getIdentity().getClusterId() == id
                && entry.type == RecordOperation.CREATED) {
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
    for (var indexName : indexEntries.keySet()) {
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(indexName);
    }
    return list;
  }

  public Map<String, FrontendTransactionIndexChanges> getIndexOperations() {
    return indexEntries;
  }

  public FrontendTransactionIndexChanges getIndexChangesInternal(final String indexName) {
    if (getDatabaseSession().isRemote()) {
      return null;
    }
    return getIndexChanges(indexName);
  }

  @Override
  public void addIndexEntry(
      final IndexInternal index,
      final String iIndexName,
      final OPERATION iOperation,
      final Object key,
      final Identifiable value) {
    // index changes are tracked on server in case of client-server deployment
    assert session.getStorage() instanceof AbstractPaginatedStorage;

    changed = true;
    try {
      var indexEntry = indexEntries.get(iIndexName);
      if (indexEntry == null) {
        indexEntry = new FrontendTransactionIndexChanges(index);
        indexEntries.put(iIndexName, indexEntry);
      }

      if (iOperation == OPERATION.CLEAR) {
        indexEntry.setCleared();
      } else {
        var changes = indexEntry.getChangesPerKey(key);
        changes.add(value, iOperation);

        if (changes.key == key
            && key instanceof ChangeableIdentity changeableIdentity
            && changeableIdentity.canChangeIdentity()) {
          changeableIdentity.addIdentityChangeListener(indexEntry);
        }

        if (value == null) {
          return;
        }

        var transactionIndexOperations =
            recordIndexOperations.get(value.getIdentity());

        if (transactionIndexOperations == null) {
          transactionIndexOperations = new ArrayList<>();
          recordIndexOperations.put(((RecordId) value.getIdentity()).copy(),
              transactionIndexOperations);
        }

        transactionIndexOperations.add(
            new FrontendTransactionRecordIndexOperation(iIndexName, key, iOperation));
      }
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  /**
   * Buffer sizes index changes to be flushed at commit time.
   */
  public FrontendTransactionIndexChanges getIndexChanges(final String iIndexName) {
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
    for (final var v : recordOperations.values()) {
      final var rec = v.record;
      RecordInternal.unsetDirty(rec);
      rec.unload();
    }

    var localCache = session.getLocalCache();
    localCache.unloadRecords();
    localCache.clear();
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    if (txStartCounter < 0) {
      throw new TransactionException(session, "Invalid value of TX counter");
    }
    checkTransactionValid();

    txStartCounter += commitLevelDiff;
    status = TXSTATUS.ROLLBACKING;

    if (!force && txStartCounter > 0) {
      return;
    }

    if (session.isRemote()) {
      final var storage = session.getStorage();
      ((StorageProxy) storage).rollback(FrontendTransactionOptimistic.this);
    }

    internalRollback();
  }

  @Override
  public boolean exists(RID rid) {
    checkTransactionValid();

    final DBRecord txRecord = getRecord(rid);
    if (txRecord == FrontendTransactionAbstract.DELETED_RECORD) {
      return false;
    }

    if (txRecord != null) {
      return true;
    }

    return session.executeExists(rid);
  }

  @Override
  public @Nonnull DBRecord loadRecord(RID rid) {

    checkTransactionValid();

    final var txRecord = getRecord(rid);
    if (txRecord == FrontendTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      throw new RecordNotFoundException(session, rid);
    }

    if (txRecord != null) {
      return txRecord;
    }

    if (rid.isTemporary()) {
      throw new RecordNotFoundException(session, rid);
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    return session.executeReadRecord((RecordId) rid);
  }

  public void deleteRecord(final RecordAbstract record) {
    try {
      addRecordOperation(record, RecordOperation.DELETED, null);
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  public DBRecord saveRecord(RecordAbstract passedRecord, final String clusterName) {
    try {
      if (passedRecord == null) {
        return null;
      }
      if (passedRecord.isUnloaded()) {
        throw new DatabaseException(session,
            "Record "
                + passedRecord
                + " is not bound to session, please call "
                + DatabaseSession.class.getSimpleName()
                + ".bindToSession(record) before changing it");
      }

      // fetch primary record if the record is a proxy record.
      passedRecord = passedRecord.getRecord(session);
      final var operation =
          passedRecord.getIdentity().isValid()
              ? RecordOperation.UPDATED
              : RecordOperation.CREATED;

      addRecordOperation(passedRecord, operation, clusterName);
      return passedRecord;
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  @Override
  public String toString() {
    return "FrontendTransactionOptimistic [id="
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

  public void addRecordOperation(RecordAbstract record, byte status, String clusterName) {
    try {
      validateState(record);
      var rid = record.getIdentity();

      if (clusterName == null) {
        clusterName = session.getClusterNameById(record.getIdentity().getClusterId());
      }

      if (!rid.isValid()) {
        session.assignAndCheckCluster(record, clusterName);
        rid.setClusterPosition(newRecordsPositionsGenerator--);
      }

      var txEntry = getRecordEntry(rid);
      try {
        if (txEntry == null) {
          if (rid.isTemporary() && status == RecordOperation.UPDATED) {
            throw new IllegalStateException(
                "Temporary records can not be added to the transaction");
          }
          txEntry = new RecordOperation(record, status);

          recordOperations.put(rid.copy(), txEntry);
          changed = true;
        } else {
          if (txEntry.record != record) {
            throw new TransactionException(session,
                "Found record in transaction with the same RID but different instance");
          }

          switch (txEntry.type) {
            case RecordOperation.UPDATED:
              if (status == RecordOperation.DELETED) {
                txEntry.type = RecordOperation.DELETED;
                changed = true;
              } else if (status == RecordOperation.CREATED) {
                throw new IllegalStateException(
                    "Invalid operation, record can not be created as it is already updated");
              }
              break;
            case RecordOperation.DELETED:
              if (status == RecordOperation.UPDATED || status == RecordOperation.CREATED) {
                throw new IllegalStateException(
                    "Invalid operation, record can not be updated or created as it is already deleted");
              }
              break;
            case RecordOperation.CREATED:
              if (status == RecordOperation.DELETED) {
                recordOperations.remove(rid);
                changed = true;
              } else if (status == RecordOperation.CREATED) {
                throw new IllegalStateException(
                    "Invalid operation, record can not be created as it is already created");
              }
              break;
          }
        }

        switch (status) {
          case RecordOperation.CREATED: {
            if (record instanceof EntityImpl entity) {
              SchemaImmutableClass clazz = null;
              clazz = entity.getImmutableSchemaClass(session);
              if (clazz != null) {
                ClassIndexManager.checkIndexesAfterCreate(entity, session);
                txEntry.indexTrackingDirtyCounter = record.getDirtyCounter();
              }
            }
          }
          break;
          case RecordOperation.UPDATED: {
            if (record instanceof EntityImpl entity) {
              SchemaImmutableClass clazz = null;
              clazz = entity.getImmutableSchemaClass(session);
              if (clazz != null && record.getDirtyCounter() != txEntry.indexTrackingDirtyCounter) {
                ClassIndexManager.checkIndexesAfterUpdate(entity, session);
                txEntry.indexTrackingDirtyCounter = record.getDirtyCounter();
              }
            }
          }
          break;
          case RecordOperation.DELETED: {
            if (record instanceof EntityImpl entity) {
              SchemaImmutableClass clazz = null;
              clazz = entity.getImmutableSchemaClass(session);

              if (clazz != null) {
                ClassIndexManager.checkIndexesAfterDelete(entity, session);

                if (clazz.isSequence()) {
                  SequenceLibraryImpl.onAfterSequenceDropped(this, entity);
                } else if (clazz.isFunction()) {
                  FunctionLibraryImpl.onAfterFunctionDropped(this, entity);
                } else if (clazz.isScheduler()) {
                  SchedulerImpl.onAfterEventDropped(this, entity);
                }
              }
            }
          }
          break;
          default:
            throw new IllegalStateException(
                "Invalid transaction operation type " + status);
        }

        if (record instanceof EntityImpl && ((EntityImpl) record).isTrackingChanges()) {
          EntityInternalUtils.clearTrackData(((EntityImpl) record));
        }
      } catch (final Exception e) {
        throw BaseException.wrapException(
            new DatabaseException(session,
                "Error on execution of operation on record " + record.getIdentity()), e, session);
      }
    } catch (Exception e) {
      rollback(true, 0);
      throw e;
    }
  }

  private void validateState(RecordAbstract record) {
    if (record.isUnloaded()) {
      throw new DatabaseException(session,
          "Record "
              + record
              + " is not bound to session, please call "
              + DatabaseSession.class.getSimpleName()
              + ".bindToSession(record) before changing it");
    }
    if (record.isEmbedded()) {
      throw new DatabaseException(session,
          "Record "
              + record
              + " is embedded and can not added to list of records to be saved");
    }
    checkTransactionValid();
  }

  public void deleteRecordOperation(RecordAbstract record) {
    var identity = record.getIdentity();

    if (generatedOriginalRecordIdMap.containsKey(identity)) {
      throw new TransactionException(session,
          "Cannot delete record operation for record with identity " + identity
              + " because it was updated during transaction");
    }

    recordOperations.remove(identity);
  }

  private void doCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
      if (status == TXSTATUS.ROLLBACKING) {
        internalRollback();
      }

      throw new RollbackException(
          "Given transaction was rolled back, and thus cannot be committed.");
    }

    try {
      status = TXSTATUS.COMMITTING;
      if (sentToServer || !recordOperations.isEmpty() || !indexEntries.isEmpty()) {
        session.internalCommit(this);
        try {
          session.afterCommitOperations();
        } catch (Exception e) {
          LogManager.instance().error(this,
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

  private void preProcessRecordsAndExecuteCallCallbacks() {
    var serializer = session.getSerializer();

    while (changed) {
      changed = false;

      var operations = new ArrayList<>(recordOperations.values());
      for (var recordOperation : operations) {
        var record = recordOperation.record;
        if (recordOperation.type == RecordOperation.CREATED
            || recordOperation.type == RecordOperation.UPDATED) {
          if (recordOperation.record instanceof EntityImpl entity) {
            var className = entity.getSchemaClassName();
            if (recordOperation.recordCallBackDirtyCounter != record.getDirtyCounter()) {
              EntityInternalUtils.checkClass(entity, session);
              try {
                entity.autoConvertValues();
              } catch (ValidationException e) {
                entity.undo();
                throw e;
              }

              EntityInternalUtils.convertAllMultiValuesToTrackedVersions(entity);

              if (recordOperation.type == RecordOperation.CREATED) {
                if (className != null) {
                  session.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_CREATE,
                      className);
                }
              } else {
                // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
                if (className != null) {
                  session.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_UPDATE,
                      className);
                }
              }
            }

            entity.recordFormat = serializer;

            if (entity.getDirtyCounter() != recordOperation.indexTrackingDirtyCounter) {
              if (className != null) {
                ClassIndexManager.checkIndexesAfterUpdate(entity, session);
                recordOperation.indexTrackingDirtyCounter = record.getDirtyCounter();
              }
            }
          }

          if (recordOperation.recordCallBackDirtyCounter != record.getDirtyCounter()) {
            if (recordOperation.type == RecordOperation.CREATED) {
              if (recordOperation.recordCallBackDirtyCounter == 0) {
                if (processRecordCreation(recordOperation, record)) {
                  changed = true;
                }
              } else if (processRecordUpdate(recordOperation, record)) {
                changed = true;
              }
            } else {
              if (processRecordUpdate(recordOperation, record)) {
                changed = true;
              }
            }
          }
        } else if (recordOperation.type == RecordOperation.DELETED) {
          if (record.getDirtyCounter() != recordOperation.recordCallBackDirtyCounter) {
            processRecordDeletion(recordOperation, record);
          }
        } else {
          throw new IllegalStateException("Invalid record operation type " + recordOperation.type);
        }
      }
    }
  }

  private void processRecordDeletion(RecordOperation recordOperation, RecordAbstract record) {
    var clusterName = session.getClusterNameById(record.getIdentity().getClusterId());

    session.beforeDeleteOperations(record, clusterName);
    try {
      session.afterDeleteOperations(record);
      if (record instanceof EntityImpl && ((EntityImpl) record).isTrackingChanges()) {
        EntityInternalUtils.clearTrackData(((EntityImpl) record));
      }
    } catch (Exception e) {
      session.callbackHooks(TYPE.DELETE_FAILED, record);
      throw e;
    } finally {
      session.callbackHooks(TYPE.FINALIZE_DELETION, record);
    }
    recordOperation.recordCallBackDirtyCounter = record.getDirtyCounter();
  }

  private boolean processRecordUpdate(RecordOperation recordOperation, RecordAbstract record) {
    var dirtyCounter = record.getDirtyCounter();
    var clusterName = session.getClusterNameById(record.getIdentity().getClusterId());

    recordOperation.recordCallBackDirtyCounter = dirtyCounter;
    session.beforeUpdateOperations(record, clusterName);
    try {
      session.afterUpdateOperations(record);
      if (record instanceof EntityImpl && ((EntityImpl) record).isTrackingChanges()) {
        EntityInternalUtils.clearTrackData(((EntityImpl) record));
      }
    } catch (Exception e) {
      session.callbackHooks(TYPE.UPDATE_FAILED, record);
      throw e;
    } finally {
      session.callbackHooks(TYPE.FINALIZE_UPDATE, record);
    }

    return record.getDirtyCounter() != recordOperation.recordCallBackDirtyCounter;
  }

  private boolean processRecordCreation(RecordOperation recordOperation, RecordAbstract record) {
    session.assignAndCheckCluster(recordOperation.record, null);
    var clusterName = session.getClusterNameById(record.getIdentity().getClusterId());

    recordOperation.recordCallBackDirtyCounter = record.getDirtyCounter();
    session.beforeCreateOperations(record, clusterName);
    try {
      session.afterCreateOperations(record);
      if (record instanceof EntityImpl && ((EntityImpl) record).isTrackingChanges()) {
        EntityInternalUtils.clearTrackData(((EntityImpl) record));
      }
    } catch (Exception e) {
      session.callbackHooks(TYPE.CREATE_FAILED, record);
      throw e;
    } finally {
      session.callbackHooks(TYPE.FINALIZE_CREATION, record);
    }

    return recordOperation.recordCallBackDirtyCounter != record.getDirtyCounter();
  }

  public void resetChangesTracking() {
    isAlreadyStartedOnServer = true;
    changed = false;
  }

  @Override
  public void close() {
    final var dbCache = session.getLocalCache();
    for (var txEntry : recordOperations.values()) {
      var record = txEntry.record;

      if (!record.isUnloaded()) {
        if (record instanceof EntityImpl entity) {
          EntityInternalUtils.clearTransactionTrackData(entity);
        }

        RecordInternal.unsetDirty(record);
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

    session.setDefaultTransactionMode();
    userData.clear();
  }

  public void updateIdentityAfterCommit(final RecordId oldRid, final RecordId newRid) {
    if (oldRid.equals(newRid))
    // NO CHANGE, IGNORE IT
    {
      return;
    }

    // XXX: Identity update may mutate the index keys, so we have to identify and reinsert
    // potentially affected index keys to keep
    // the FrontendTransactionIndexChanges.changesPerKey in a consistent state.

    final List<KeyChangesUpdateRecord> keyRecordsToReinsert = new ArrayList<>();
    final var database = getDatabaseSession();
    if (!database.isRemote()) {
      final var indexManager = database.getMetadata().getIndexManagerInternal();
      for (var entry : indexEntries.entrySet()) {
        final var index = indexManager.getIndex(database, entry.getKey());
        if (index == null) {
          throw new TransactionException(session,
              "Cannot find index '" + entry.getValue() + "' while committing transaction");
        }

        final var fieldRidDependencies = getIndexFieldRidDependencies(index);
        if (!isIndexMayDependOnRids(fieldRidDependencies)) {
          continue;
        }

        final var indexChanges = entry.getValue();
        for (final var iterator =
            indexChanges.changesPerKey.values().iterator();
            iterator.hasNext(); ) {
          final var keyChanges = iterator.next();
          if (isIndexKeyMayDependOnRid(keyChanges.key, oldRid, fieldRidDependencies)) {
            keyRecordsToReinsert.add(new KeyChangesUpdateRecord(keyChanges, indexChanges));
            iterator.remove();

            if (keyChanges.key instanceof ChangeableIdentity changeableIdentity) {
              changeableIdentity.removeIdentityChangeListener(indexChanges);
            }
          }
        }
      }
    }

    // Update the identity.

    final var rec = getRecordEntry(oldRid);
    if (rec != null) {
      generatedOriginalRecordIdMap.put(newRid.copy(), oldRid.copy());

      if (!rec.record.getIdentity().equals(newRid)) {
        final var recordId = rec.record.getIdentity();
        recordId.setClusterPosition(newRid.getClusterPosition());
        recordId.setClusterId(newRid.getClusterId());
      }
    }

    // Reinsert the potentially affected index keys.

    for (var record : keyRecordsToReinsert) {
      record.indexChanges.changesPerKey.put(record.keyChanges.key, record.keyChanges);
    }

    // Update the indexes.

    var val = getRecordEntry(oldRid);
    final var transactionIndexOperations =
        recordIndexOperations.get(val != null ? val.getRecordId() : null);
    if (transactionIndexOperations != null) {
      for (final var indexOperation : transactionIndexOperations) {
        var indexEntryChanges = indexEntries.get(indexOperation.index);
        if (indexEntryChanges == null) {
          continue;
        }
        final FrontendTransactionIndexChangesPerKey keyChanges;
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

  private static void updateChangesIdentity(
      RID oldRid, RID newRid, FrontendTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null) {
      return;
    }

    for (final var indexEntry : changesPerKey.getEntriesAsList()) {
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

  private static Dependency[] getIndexFieldRidDependencies(Index index) {
    final var definition = index.getDefinition();

    if (definition == null) { // type for untyped index is still not resolved
      return null;
    }

    final var types = definition.getTypes();
    final var dependencies = new Dependency[types.length];

    for (var i = 0; i < types.length; ++i) {
      dependencies[i] = getTypeRidDependency(types[i]);
    }

    return dependencies;
  }

  private static boolean isIndexMayDependOnRids(Dependency[] fieldDependencies) {
    if (fieldDependencies == null) {
      return true;
    }

    for (var dependency : fieldDependencies) {
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
      Object key, RID rid, Dependency[] keyDependencies) {
    if (key instanceof CompositeKey) {
      final var subKeys = ((CompositeKey) key).getKeys();
      for (var i = 0; i < subKeys.size(); ++i) {
        if (isIndexKeyMayDependOnRid(
            subKeys.get(i), rid, keyDependencies == null ? null : keyDependencies[i])) {
          return true;
        }
      }
      return false;
    }

    return isIndexKeyMayDependOnRid(key, rid, keyDependencies == null ? null : keyDependencies[0]);
  }

  private static boolean isIndexKeyMayDependOnRid(Object key, RID rid, Dependency dependency) {
    if (dependency == Dependency.No) {
      return false;
    }

    if (key instanceof Identifiable) {
      return key.equals(rid);
    }

    return dependency == Dependency.Unknown || dependency == null;
  }

  private static Dependency getTypeRidDependency(PropertyType type) {
    // fallback to the safest variant, just in case
    return switch (type) {
      case ANY -> Dependency.Unknown;
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

    final FrontendTransactionIndexChangesPerKey keyChanges;
    final FrontendTransactionIndexChanges indexChanges;

    KeyChangesUpdateRecord(
        FrontendTransactionIndexChangesPerKey keyChanges,
        FrontendTransactionIndexChanges indexChanges) {
      this.keyChanges = keyChanges;
      this.indexChanges = indexChanges;
    }
  }

  protected void checkTransactionValid() {
    if (status == TXSTATUS.INVALID) {
      throw new TransactionException(session,
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

  public Collection<RecordOperation> getCurrentRecordEntries() {
    return recordOperations.values();
  }

  public Collection<RecordOperation> getRecordOperations() {
    return recordOperations.values();
  }

  public RecordOperation getRecordEntry(RID ridPar) {
    assert ridPar instanceof RecordId;

    var rid = ridPar;
    RecordOperation entry;
    do {
      entry = recordOperations.get(rid);
      if (entry == null) {
        rid = generatedOriginalRecordIdMap.get(rid);
      }
    } while (entry == null && rid != null && !rid.equals(ridPar));

    return entry;
  }

  public Map<RecordId, RecordId> getGeneratedOriginalRecordIdMap() {
    return generatedOriginalRecordIdMap;
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
  public void setMetadataHolder(FrontendTransacationMetadataHolder metadata) {
    this.metadata = metadata;
  }

  public Iterator<byte[]> getSerializedOperations() {
    if (serializedOperations != null) {
      return serializedOperations.iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  public int getTxStartCounter() {
    return txStartCounter;
  }
}
