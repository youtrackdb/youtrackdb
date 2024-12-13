package com.jetbrains.youtrack.db.internal.server.tx;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.api.exception.TransactionException;
import com.jetbrains.youtrack.db.internal.core.index.ClassIndexManager;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionOptimistic;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TransactionOptimisticServer extends TransactionOptimistic {

  public TransactionOptimisticServer(DatabaseSessionInternal database, long txId) {
    super(database, txId);
  }

  public void mergeReceivedTransaction(List<RecordOperationRequest> operations) {
    if (operations == null) {
      return;
    }

    // SORT OPERATIONS BY TYPE TO BE SURE THAT CREATES ARE PROCESSED FIRST
    operations.sort(Comparator.comparingInt(RecordOperationRequest::getType).reversed());

    final HashMap<RID, RecordOperation> tempEntries = new LinkedHashMap<>();
    final HashMap<RID, RecordOperation> createdRecords = new HashMap<>();
    final HashMap<RecordId, RecordAbstract> updatedRecords = new HashMap<>();

    try {
      List<RecordOperation> toMergeUpdates = new ArrayList<>();
      for (RecordOperationRequest operation : operations) {
        final byte recordStatus = operation.getType();

        final RecordId rid = (RecordId) operation.getId();

        final RecordOperation entry;

        switch (recordStatus) {
          case RecordOperation.CREATED:
            RecordAbstract record =
                YouTrackDBEnginesManager.instance()
                    .getRecordFactoryManager()
                    .newInstance(operation.getRecordType(), rid, getDatabase());
            RecordSerializerNetworkV37.INSTANCE.fromStream(getDatabase(), operation.getRecord(),
                record);
            entry = new RecordOperation(record, RecordOperation.CREATED);
            RecordInternal.setVersion(record, 0);

            createdRecords.put(rid.copy(), entry);
            break;

          case RecordOperation.UPDATED:
            byte type = operation.getRecordType();
            if (type == DocumentSerializerDelta.DELTA_RECORD_TYPE) {
              int version = operation.getVersion();
              EntityImpl updated;
              try {
                updated = database.load(rid);
              } catch (RecordNotFoundException rnf) {
                updated = new EntityImpl();
              }

              updated.deserializeFields();
              EntityInternalUtils.clearTransactionTrackData(updated);
              DocumentSerializerDelta delta = DocumentSerializerDelta.instance();
              delta.deserializeDelta(getDatabase(), operation.getRecord(), updated);
              entry = new RecordOperation(updated, RecordOperation.UPDATED);
              RecordInternal.setIdentity(updated, rid);
              RecordInternal.setVersion(updated, version);
              updated.setDirty();
              RecordInternal.setContentChanged(entry.record, operation.isContentChanged());
              updatedRecords.put(rid, updated);
            } else {
              int version = operation.getVersion();
              var updated =
                  YouTrackDBEnginesManager.instance()
                      .getRecordFactoryManager()
                      .newInstance(operation.getRecordType(), rid, getDatabase());
              RecordSerializerNetworkV37.INSTANCE.fromStream(getDatabase(), operation.getRecord(),
                  updated);
              entry = new RecordOperation(updated, RecordOperation.UPDATED);
              RecordInternal.setVersion(updated, version);
              updated.setDirty();
              RecordInternal.setContentChanged(entry.record, operation.isContentChanged());
              toMergeUpdates.add(entry);
            }
            break;

          case RecordOperation.DELETED:
            // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
            var recordEntry = getRecordEntry(rid);
            if (recordEntry != null && recordEntry.type == RecordOperation.DELETED) {
              // ALREADY DELETED
              continue;
            }

            RecordAbstract rec = rid.getRecord();
            entry = new RecordOperation(rec, RecordOperation.DELETED);
            int deleteVersion = operation.getVersion();
            RecordInternal.setVersion(rec, deleteVersion);
            entry.record = rec;
            break;
          default:
            throw new TransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.record.getIdentity(), entry);
      }

      for (RecordOperation update : toMergeUpdates) {
        // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING
        // HOOKS (LIKE INDEXES)
        var record = update.record.getRecord();
        final boolean contentChanged = RecordInternal.isContentChanged(record);

        final RecordAbstract loadedRecord = record.getIdentity().copy().getRecord();
        if (RecordInternal.getRecordType(loadedRecord) == EntityImpl.RECORD_TYPE
            && RecordInternal.getRecordType(loadedRecord)
            == RecordInternal.getRecordType(record)) {
          ((EntityImpl) loadedRecord).merge((EntityImpl) record, false, false);

          loadedRecord.setDirty();
          RecordInternal.setContentChanged(loadedRecord, contentChanged);

          RecordInternal.setVersion(loadedRecord, record.getVersion());
          update.record = loadedRecord;
        }
      }

      var txOperations = new ArrayList<RecordOperation>(tempEntries.size());
      try {
        for (Map.Entry<RID, RecordOperation> entry : tempEntries.entrySet()) {
          var cachedRecord = database.getLocalCache().findRecord(entry.getKey());

          var operation = entry.getValue();
          var rec = operation.record;

          if (rec != cachedRecord) {
            if (cachedRecord != null) {
              rec.copyTo(cachedRecord);
            } else {
              database.getLocalCache().updateRecord(rec.getRecord());
            }
          }

          txOperations.add(preAddRecord(operation.record, entry.getValue().type));
        }

        for (var operation : txOperations) {
          postAddRecord(operation.record, operation.type, operation.callHooksOnServerTx);
        }
      } finally {
        for (var operation : txOperations) {
          finalizeAddRecord(operation.record, operation.type, operation.callHooksOnServerTx);
        }
      }

      tempEntries.clear();

      newRecordsPositionsGenerator = (createdRecords.size() + 2) * -1;
      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (RecordOperation recordOperation : createdRecords.values()) {
        var record = recordOperation.record;
        unmarshallRecord(record);
        if (record instanceof EntityImpl) {
          // Force conversion of value to class for trigger default values.
          EntityInternalUtils.autoConvertValueToClass(getDatabase(), (EntityImpl) record);
        }
      }
      for (Record record : updatedRecords.values()) {
        unmarshallRecord(record);
      }
    } catch (Exception e) {
      rollback();
      throw BaseException.wrapException(
          new SerializationException(
              "Cannot read transaction record from the network. Transaction aborted"),
          e);
    }
  }

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  protected void unmarshallRecord(final Record iRecord) {
    if (iRecord instanceof EntityImpl) {
      ((EntityImpl) iRecord).deserializeFields();
    }
  }

  private boolean checkCallHooks(RID id, byte type) {
    RecordOperation entry = recordOperations.get(id);
    return entry == null || entry.type != type;
  }

  private RecordOperation preAddRecord(RecordAbstract record, final byte iStatus) {
    changed = true;
    checkTransactionValid();

    boolean callHooks = checkCallHooks(record.getIdentity(), iStatus);
    if (callHooks) {
      switch (iStatus) {
        case RecordOperation.CREATED: {
          Identifiable res = database.beforeCreateOperations(record, null);
          if (res != null) {
            record = (RecordAbstract) res;
          }
        }
        break;
        case RecordOperation.UPDATED: {
          Identifiable res = database.beforeUpdateOperations(record, null);
          if (res != null) {
            record = (RecordAbstract) res;
          }
        }
        break;

        case RecordOperation.DELETED:
          database.beforeDeleteOperations(record, null);
          break;
      }
    }
    try {
      final RecordId rid = record.getIdentity();

      if (!rid.isPersistent() && !rid.isTemporary()) {
        RecordId oldRid = rid.copy();
        if (rid.getClusterPosition() == RecordId.CLUSTER_POS_INVALID) {
          rid.setClusterPosition(newRecordsPositionsGenerator--);
          txGeneratedRealRecordIdMap.put(rid, oldRid);
        }
      }

      RecordOperation txEntry = getRecordEntry(rid);

      if (txEntry == null) {
        // NEW ENTRY: JUST REGISTER IT
        byte status = iStatus;
        if (status == RecordOperation.UPDATED && record.getIdentity().isTemporary()) {
          status = RecordOperation.CREATED;
        }
        txEntry = new RecordOperation(record, status);
        recordOperations.put(rid.copy(), txEntry);
      } else {
        // that is important to keep links to rids of this record inside transaction to be updated
        // after commit
        RecordInternal.setIdentity(record, txEntry.record.getIdentity());
        // UPDATE PREVIOUS STATUS
        txEntry.record = record;

        switch (txEntry.type) {
          case RecordOperation.UPDATED:
            if (iStatus == RecordOperation.DELETED) {
              txEntry.type = RecordOperation.DELETED;
            }
            break;
          case RecordOperation.DELETED:
            break;
          case RecordOperation.CREATED:
            if (iStatus == RecordOperation.DELETED) {
              recordOperations.remove(rid);
              // txEntry.type = RecordOperation.DELETED;
            }
            break;
        }
      }

      if (!rid.isPersistent() && !rid.isTemporary()) {
        RecordId oldRid = rid.copy();
        if (rid.getClusterId() == RecordId.CLUSTER_ID_INVALID) {
          database.assignAndCheckCluster(record, null);
          txGeneratedRealRecordIdMap.put(rid, oldRid);
        }
      }

      txEntry.callHooksOnServerTx = callHooks;
      return txEntry;
    } catch (Exception e) {
      if (callHooks) {
        switch (iStatus) {
          case RecordOperation.CREATED:
            database.callbackHooks(RecordHook.TYPE.CREATE_FAILED, record);
            break;
          case RecordOperation.UPDATED:
            database.callbackHooks(RecordHook.TYPE.UPDATE_FAILED, record);
            break;
          case RecordOperation.DELETED:
            database.callbackHooks(RecordHook.TYPE.DELETE_FAILED, record);
            break;
        }
      }

      throw BaseException.wrapException(
          new DatabaseException("Error on saving record " + record.getIdentity()), e);
    }
  }

  private void postAddRecord(Record record, final byte iStatus, boolean callHooks) {
    checkTransactionValid();
    try {
      if (callHooks) {
        switch (iStatus) {
          case RecordOperation.CREATED:
            database.afterCreateOperations(record);
            break;
          case RecordOperation.UPDATED:
            database.afterUpdateOperations(record);
            break;
          case RecordOperation.DELETED:
            database.afterDeleteOperations(record);
            break;
        }
      } else {
        switch (iStatus) {
          case RecordOperation.CREATED:
            if (record instanceof EntityImpl) {
              ClassIndexManager.checkIndexesAfterCreate((EntityImpl) record, getDatabase());
            }
            break;
          case RecordOperation.UPDATED:
            if (record instanceof EntityImpl) {
              ClassIndexManager.checkIndexesAfterUpdate((EntityImpl) record, getDatabase());
            }
            break;
          case RecordOperation.DELETED:
            if (record instanceof EntityImpl) {
              ClassIndexManager.checkIndexesAfterDelete((EntityImpl) record, getDatabase());
            }
            break;
        }
      }
      // RESET TRACKING
      if (record instanceof EntityImpl && ((EntityImpl) record).isTrackingChanges()) {
        EntityInternalUtils.clearTrackData(((EntityImpl) record));
      }

    } catch (Exception e) {
      if (callHooks) {
        switch (iStatus) {
          case RecordOperation.CREATED:
            database.callbackHooks(RecordHook.TYPE.CREATE_FAILED, record);
            break;
          case RecordOperation.UPDATED:
            database.callbackHooks(RecordHook.TYPE.UPDATE_FAILED, record);
            break;
          case RecordOperation.DELETED:
            database.callbackHooks(RecordHook.TYPE.DELETE_FAILED, record);
            break;
        }
      }

      throw BaseException.wrapException(
          new DatabaseException("Error on saving record " + record.getIdentity()), e);
    }
  }

  private void finalizeAddRecord(Record record, final byte iStatus, boolean callHooks) {
    checkTransactionValid();
    if (callHooks) {
      switch (iStatus) {
        case RecordOperation.CREATED:
          database.callbackHooks(RecordHook.TYPE.FINALIZE_CREATION, record);
          break;
        case RecordOperation.UPDATED:
          database.callbackHooks(RecordHook.TYPE.FINALIZE_UPDATE, record);
          break;
        case RecordOperation.DELETED:
          database.callbackHooks(RecordHook.TYPE.FINALIZE_DELETION, record);
          break;
      }
    }
  }
}
