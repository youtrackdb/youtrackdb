package com.jetbrains.youtrack.db.internal.client.remote.db;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.hook.RecordHook;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionOptimistic;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperation38Response;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TransactionOptimisticClient extends TransactionOptimistic {

  private final Set<String> indexChanged = new HashSet<>();

  public TransactionOptimisticClient(DatabaseSessionInternal iDatabase) {
    super(iDatabase);
  }

  public void replaceContent(List<RecordOperation38Response> operations) {

    Map<RID, RecordOperation> oldEntries = this.recordOperations;
    this.recordOperations = new LinkedHashMap<>();
    int createCount = -2; // Start from -2 because temporary rids start from -2
    var db = getDatabase();
    for (RecordOperation38Response operation : operations) {
      if (!operation.getOldId().equals(operation.getId())) {
        txGeneratedRealRecordIdMap.put(operation.getId().copy(), operation.getOldId());
      }

      RecordAbstract record = null;
      RecordOperation op = oldEntries.get(operation.getOldId());
      if (op != null) {
        record = op.record;
      }
      var localCache = db.getLocalCache();
      if (record == null) {
        record = localCache.findRecord(operation.getOldId());
      }
      if (record != null) {
        RecordInternal.unsetDirty(record);
        record.unload();
      } else {
        record =
            YouTrackDBManager.instance()
                .getRecordFactoryManager()
                .newInstance(operation.getRecordType(), operation.getOldId(), database);
        RecordInternal.unsetDirty(record);
      }
      if (operation.getType() == RecordOperation.UPDATED
          && operation.getRecordType() == EntityImpl.RECORD_TYPE) {
        record.incrementLoading();
        try {
          record.setup(db);
          // keep rid instance to support links consistency
          record.fromStream(operation.getOriginal());
          DocumentSerializerDelta deltaSerializer = DocumentSerializerDelta.instance();
          deltaSerializer.deserializeDelta(db, operation.getRecord(),
              (EntityImpl) record);
        } finally {
          record.decrementLoading();
        }
      } else {
        record.setup(db);
        record.fromStream(operation.getRecord());
      }

      var rid = (RecordId) record.getIdentity();
      var operationId = operation.getId();
      rid.setClusterId(operationId.getClusterId());
      rid.setClusterPosition(operationId.getClusterPosition());

      RecordInternal.setVersion(record, operation.getVersion());
      RecordInternal.setContentChanged(record, operation.isContentChanged());
      if (operation.getType() == RecordOperation.UPDATED
          || operation.getType() == RecordOperation.CREATED) {
        localCache.updateRecord(record);
      } else if (operation.getType() == RecordOperation.DELETED) {
        localCache.deleteRecord(operation.getOldId());
      } else {
        throw new IllegalStateException("Unsupported operation type: " + operation.getType());
      }

      boolean callHook = checkCallHook(oldEntries, operation.getId(), operation.getType());
      addRecord(record, operation.getType(), null, callHook);
      if (operation.getType() == RecordOperation.CREATED) {
        createCount--;
      }
    }
    newRecordsPositionsGenerator = createCount;
  }

  private boolean checkCallHook(Map<RID, RecordOperation> oldEntries, RID rid, byte type) {
    RecordOperation val = oldEntries.get(rid);
    return val == null || val.type != type;
  }

  public void addRecord(
      RecordAbstract iRecord, final byte iStatus, final String iClusterName, boolean callHook) {
    try {
      if (callHook) {
        switch (iStatus) {
          case RecordOperation.CREATED: {
            Identifiable res = database.beforeCreateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (RecordAbstract) res;
              changed = true;
            }
          }
          break;
          case RecordOperation.UPDATED: {
            Identifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (RecordAbstract) res;
              changed = true;
            }
          }
          break;

          case RecordOperation.DELETED:
            database.beforeDeleteOperations(iRecord, iClusterName);
            break;
        }
      }
      try {
        final RecordId rid = (RecordId) iRecord.getIdentity();
        RecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != RecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new RecordOperation(iRecord, iStatus);
            recordOperations.put(rid.copy(), txEntry);
          }
        } else {
          // UPDATE PREVIOUS STATUS
          txEntry.record = iRecord;

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
        if (callHook) {
          switch (iStatus) {
            case RecordOperation.CREATED:
              database.callbackHooks(RecordHook.TYPE.AFTER_CREATE, iRecord);
              break;
            case RecordOperation.UPDATED:
              database.callbackHooks(RecordHook.TYPE.AFTER_UPDATE, iRecord);
              break;
            case RecordOperation.DELETED:
              database.callbackHooks(RecordHook.TYPE.AFTER_DELETE, iRecord);
              break;
          }
        }
      } catch (Exception e) {
        if (callHook) {
          switch (iStatus) {
            case RecordOperation.CREATED:
              database.callbackHooks(RecordHook.TYPE.CREATE_FAILED, iRecord);
              break;
            case RecordOperation.UPDATED:
              database.callbackHooks(RecordHook.TYPE.UPDATE_FAILED, iRecord);
              break;
            case RecordOperation.DELETED:
              database.callbackHooks(RecordHook.TYPE.DELETE_FAILED, iRecord);
              break;
          }
        }

        throw BaseException.wrapException(
            new DatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      if (callHook) {
        switch (iStatus) {
          case RecordOperation.CREATED:
            database.callbackHooks(RecordHook.TYPE.FINALIZE_CREATION, iRecord);
            break;
          case RecordOperation.UPDATED:
            database.callbackHooks(RecordHook.TYPE.FINALIZE_UPDATE, iRecord);
            break;
          case RecordOperation.DELETED:
            database.callbackHooks(RecordHook.TYPE.FINALIZE_DELETION, iRecord);
            break;
        }
      }
    }
  }

  public Set<String> getIndexChanged() {
    return indexChanged;
  }

  @Override
  public void addIndexEntry(
      Index delegate, String iIndexName, OPERATION iOperation, Object key, Identifiable iValue) {
    this.indexChanged.add(delegate.getName());
  }
}
