package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperation38Response;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
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

      YTRecordAbstract record = null;
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
          && operation.getRecordType() == YTDocument.RECORD_TYPE) {
        record.incrementLoading();
        try {
          record.setup(db);
          // keep rid instance to support links consistency
          record.fromStream(operation.getOriginal());
          ODocumentSerializerDelta deltaSerializer = ODocumentSerializerDelta.instance();
          deltaSerializer.deserializeDelta(db, operation.getRecord(),
              (YTDocument) record);
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
      YTRecordAbstract iRecord, final byte iStatus, final String iClusterName, boolean callHook) {
    try {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED: {
            YTIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (YTRecordAbstract) res;
              changed = true;
            }
          }
          break;
          case ORecordOperation.UPDATED: {
            YTIdentifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (YTRecordAbstract) res;
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
              database.callbackHooks(ORecordHook.TYPE.AFTER_CREATE, iRecord);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(ORecordHook.TYPE.AFTER_DELETE, iRecord);
              break;
          }
        }
      } catch (Exception e) {
        if (callHook) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(ORecordHook.TYPE.CREATE_FAILED, iRecord);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(ORecordHook.TYPE.UPDATE_FAILED, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(ORecordHook.TYPE.DELETE_FAILED, iRecord);
              break;
          }
        }

        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_CREATION, iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_UPDATE, iRecord);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, iRecord);
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
