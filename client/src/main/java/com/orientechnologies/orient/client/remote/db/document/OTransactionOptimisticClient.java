package com.orientechnologies.orient.client.remote.db.document;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperation38Response;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.hook.YTRecordHook;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges.OPERATION;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionOptimistic;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class OTransactionOptimisticClient extends OTransactionOptimistic {

  private final Set<String> indexChanged = new HashSet<>();

  public OTransactionOptimisticClient(YTDatabaseSessionInternal iDatabase) {
    super(iDatabase);
  }

  public void replaceContent(List<ORecordOperation38Response> operations) {

    Map<YTRID, ORecordOperation> oldEntries = this.recordOperations;
    this.recordOperations = new LinkedHashMap<>();
    int createCount = -2; // Start from -2 because temporary rids start from -2
    var db = getDatabase();
    for (ORecordOperation38Response operation : operations) {
      if (!operation.getOldId().equals(operation.getId())) {
        txGeneratedRealRecordIdMap.put(operation.getId().copy(), operation.getOldId());
      }

      RecordAbstract record = null;
      ORecordOperation op = oldEntries.get(operation.getOldId());
      if (op != null) {
        record = op.record;
      }
      var localCache = db.getLocalCache();
      if (record == null) {
        record = localCache.findRecord(operation.getOldId());
      }
      if (record != null) {
        ORecordInternal.unsetDirty(record);
        record.unload();
      } else {
        record =
            YouTrackDBManager.instance()
                .getRecordFactoryManager()
                .newInstance(operation.getRecordType(), operation.getOldId(), database);
        ORecordInternal.unsetDirty(record);
      }
      if (operation.getType() == ORecordOperation.UPDATED
          && operation.getRecordType() == EntityImpl.RECORD_TYPE) {
        record.incrementLoading();
        try {
          record.setup(db);
          // keep rid instance to support links consistency
          record.fromStream(operation.getOriginal());
          ODocumentSerializerDelta deltaSerializer = ODocumentSerializerDelta.instance();
          deltaSerializer.deserializeDelta(db, operation.getRecord(),
              (EntityImpl) record);
        } finally {
          record.decrementLoading();
        }
      } else {
        record.setup(db);
        record.fromStream(operation.getRecord());
      }

      var rid = (YTRecordId) record.getIdentity();
      var operationId = operation.getId();
      rid.setClusterId(operationId.getClusterId());
      rid.setClusterPosition(operationId.getClusterPosition());

      ORecordInternal.setVersion(record, operation.getVersion());
      ORecordInternal.setContentChanged(record, operation.isContentChanged());
      if (operation.getType() == ORecordOperation.UPDATED
          || operation.getType() == ORecordOperation.CREATED) {
        localCache.updateRecord(record);
      } else if (operation.getType() == ORecordOperation.DELETED) {
        localCache.deleteRecord(operation.getOldId());
      } else {
        throw new IllegalStateException("Unsupported operation type: " + operation.getType());
      }

      boolean callHook = checkCallHook(oldEntries, operation.getId(), operation.getType());
      addRecord(record, operation.getType(), null, callHook);
      if (operation.getType() == ORecordOperation.CREATED) {
        createCount--;
      }
    }
    newRecordsPositionsGenerator = createCount;
  }

  private boolean checkCallHook(Map<YTRID, ORecordOperation> oldEntries, YTRID rid, byte type) {
    ORecordOperation val = oldEntries.get(rid);
    return val == null || val.type != type;
  }

  public void addRecord(
      RecordAbstract iRecord, final byte iStatus, final String iClusterName, boolean callHook) {
    try {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED: {
            YTIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (RecordAbstract) res;
              changed = true;
            }
          }
          break;
          case ORecordOperation.UPDATED: {
            YTIdentifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (RecordAbstract) res;
              changed = true;
            }
          }
          break;

          case ORecordOperation.DELETED:
            database.beforeDeleteOperations(iRecord, iClusterName);
            break;
        }
      }
      try {
        final YTRecordId rid = (YTRecordId) iRecord.getIdentity();
        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(iRecord, iStatus);
            recordOperations.put(rid.copy(), txEntry);
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
                recordOperations.remove(rid);
                // txEntry.type = ORecordOperation.DELETED;
              }
              break;
          }
        }
        if (callHook) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(YTRecordHook.TYPE.AFTER_CREATE, iRecord);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(YTRecordHook.TYPE.AFTER_UPDATE, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(YTRecordHook.TYPE.AFTER_DELETE, iRecord);
              break;
          }
        }
      } catch (Exception e) {
        if (callHook) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(YTRecordHook.TYPE.CREATE_FAILED, iRecord);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(YTRecordHook.TYPE.UPDATE_FAILED, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(YTRecordHook.TYPE.DELETE_FAILED, iRecord);
              break;
          }
        }

        throw YTException.wrapException(
            new YTDatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(YTRecordHook.TYPE.FINALIZE_CREATION, iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(YTRecordHook.TYPE.FINALIZE_UPDATE, iRecord);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(YTRecordHook.TYPE.FINALIZE_DELETION, iRecord);
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
      OIndex delegate, String iIndexName, OPERATION iOperation, Object key, YTIdentifiable iValue) {
    this.indexChanged.add(delegate.getName());
  }
}
