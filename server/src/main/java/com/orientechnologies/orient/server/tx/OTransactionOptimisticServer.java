package com.orientechnologies.orient.server.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.*;
import java.util.*;

/**
 * Created by tglman on 28/12/16.
 */
public class OTransactionOptimisticServer extends OTransactionOptimistic {

  public OTransactionOptimisticServer(ODatabaseDocumentInternal database, int txId) {
    super(database, txId);
  }

  public void mergeReceivedTransaction(List<ORecordOperationRequest> operations) {
    if (operations == null) {
      return;
    }

    final HashMap<ORID, ORecordOperation> tempEntries = new LinkedHashMap<>();
    final HashMap<ORID, ORecordAbstract> createdRecords = new HashMap<>();
    final HashMap<ORecordId, ORecordAbstract> updatedRecords = new HashMap<>();

    try {
      List<ORecordOperation> toMergeUpdates = new ArrayList<>();
      for (ORecordOperationRequest operation : operations) {
        final byte recordStatus = operation.getType();

        final ORecordId rid = (ORecordId) operation.getId();

        final ORecordOperation entry;

        switch (recordStatus) {
          case ORecordOperation.CREATED:
            ORecord record =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(operation.getRecordType(), rid, getDatabase());
            ORecordSerializerNetworkV37.INSTANCE.fromStream(operation.getRecord(), record);
            entry = new ORecordOperation(record, ORecordOperation.CREATED);
            ORecordInternal.setVersion(record, 0);

            createdRecords.put(rid.copy(), entry.getRecord());
            break;

          case ORecordOperation.UPDATED:
            byte type = operation.getRecordType();
            if (type == ODocumentSerializerDelta.DELTA_RECORD_TYPE) {
              int version = operation.getVersion();
              var updated = (ORecordAbstract) database.load(rid);
              if (updated == null) {
                updated = new ODocument();
              }
              ((ODocument) updated).deserializeFields();
              ODocumentInternal.clearTransactionTrackData((ODocument) updated);
              ODocumentSerializerDelta delta = ODocumentSerializerDelta.instance();
              delta.deserializeDelta(operation.getRecord(), (ODocument) updated);
              entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
              ORecordInternal.setIdentity(updated, rid);
              ORecordInternal.setVersion(updated, version);
              updated.setDirty();
              ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
              updatedRecords.put(rid, updated);
            } else {
              int version = operation.getVersion();
              ORecord updated =
                  Orient.instance()
                      .getRecordFactoryManager()
                      .newInstance(operation.getRecordType(), rid, getDatabase());
              ORecordSerializerNetworkV37.INSTANCE.fromStream(operation.getRecord(), updated);
              entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
              ORecordInternal.setVersion(updated, version);
              updated.setDirty();
              ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
              toMergeUpdates.add(entry);
            }
            break;

          case ORecordOperation.DELETED:
            // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
            var recordEntry = getRecordEntry(rid);
            if (recordEntry != null && recordEntry.getType() == ORecordOperation.DELETED) {
              // ALREADY DELETED
              continue;
            }

            final ORecord rec = rid.getRecord();
            entry = new ORecordOperation(rec, ORecordOperation.DELETED);
            int deleteVersion = operation.getVersion();
            if (rec == null) {
              throw new ORecordNotFoundException(rid.getIdentity());
            } else {
              ORecordInternal.setVersion(rec, deleteVersion);
              entry.setRecord(rec);
            }
            break;

          default:
            throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }

      for (ORecordOperation update : toMergeUpdates) {
        // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING
        // HOOKS (LIKE INDEXES)
        final ORecord record = update.record.getRecord();
        final boolean contentChanged = ORecordInternal.isContentChanged(record);

        final ORecord loadedRecord = record.getIdentity().copy().getRecord();
        if (loadedRecord == null) {
          throw new ORecordNotFoundException(record.getIdentity());
        }

        if (ORecordInternal.getRecordType(loadedRecord) == ODocument.RECORD_TYPE
            && ORecordInternal.getRecordType(loadedRecord)
                == ORecordInternal.getRecordType(record)) {
          ((ODocument) loadedRecord).merge((ODocument) record, false, false);

          loadedRecord.setDirty();
          ORecordInternal.setContentChanged(loadedRecord, contentChanged);

          ORecordInternal.setVersion(loadedRecord, record.getVersion());
          update.record = loadedRecord;
        }
      }

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Map.Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {
        var cachedRecord = database.getLocalCache().findRecord(entry.getKey());
        var rec = entry.getValue().getRecord();
        if (rec != cachedRecord) {
          if (cachedRecord != null) {
            rec.copyTo(cachedRecord);
          } else {
            database.getLocalCache().updateRecord(rec.getRecord());
          }
        }

        addRecord(entry.getValue().getRecord(), entry.getValue().type);
      }
      tempEntries.clear();

      newRecordsPositionsGenerator = (createdRecords.size() + 2) * -1;
      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord record : createdRecords.values()) {
        unmarshallRecord(record);
        if (record instanceof ODocument) {
          // Force conversion of value to class for trigger default values.
          ODocumentInternal.autoConvertValueToClass(getDatabase(), (ODocument) record);
        }
      }
      for (ORecord record : updatedRecords.values()) {
        unmarshallRecord(record);
      }
    } catch (Exception e) {
      rollback();
      throw OException.wrapException(
          new OSerializationException(
              "Cannot read transaction record from the network. Transaction aborted"),
          e);
    }
  }

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  protected void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();
    }
  }

  private boolean checkCallHooks(ORID id, byte type) {
    ORecordOperation entry = recordOperations.get(id);
    return entry == null || entry.getType() != type;
  }

  private void addRecord(ORecord record, final byte iStatus) {
    changed = true;
    checkTransactionValid();

    boolean callHooks = checkCallHooks(record.getIdentity(), iStatus);
    try {
      if (callHooks) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            {
              OIdentifiable res = database.beforeCreateOperations(record, null);
              if (res != null) {
                record = (ORecord) res;
              }
            }
            break;
          case ORecordOperation.UPDATED:
            {
              OIdentifiable res = database.beforeUpdateOperations(record, null);
              if (res != null) {
                record = (ORecord) res;
              }
            }
            break;

          case ORecordOperation.DELETED:
            database.beforeDeleteOperations(record, null);
            break;
        }
      }
      try {
        final ORecordId rid = (ORecordId) record.getIdentity();

        if (!rid.isPersistent() && !rid.isTemporary()) {
          ORecordId oldRid = rid.copy();
          if (rid.getClusterPosition() == ORecordId.CLUSTER_POS_INVALID) {
            ORecordInternal.onBeforeIdentityChanged(record);
            rid.setClusterPosition(newRecordsPositionsGenerator--);
            txGeneratedRealRecordIdMap.put(oldRid, rid);
            ORecordInternal.onAfterIdentityChanged(record);
          }
        }

        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          // NEW ENTRY: JUST REGISTER IT
          byte status = iStatus;
          if (status == ORecordOperation.UPDATED && record.getIdentity().isTemporary()) {
            status = ORecordOperation.CREATED;
          }
          txEntry = new ORecordOperation(record, status);
          recordOperations.put(rid.copy(), txEntry);
        } else {
          // that is important to keep links to rids of this record inside transaction to be updated
          // after commit
          ORecordInternal.setIdentity(record, (ORecordId) txEntry.record.getIdentity());
          // UPDATE PREVIOUS STATUS
          txEntry.record = record;

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

        if (callHooks) {
          switch (iStatus) {
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
        } else {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              if (record instanceof ODocument) {
                OClassIndexManager.checkIndexesAfterCreate((ODocument) record, getDatabase());
              }
              break;
            case ORecordOperation.UPDATED:
              if (record instanceof ODocument) {
                OClassIndexManager.checkIndexesAfterUpdate((ODocument) record, getDatabase());
              }
              break;
            case ORecordOperation.DELETED:
              if (record instanceof ODocument) {
                OClassIndexManager.checkIndexesAfterDelete((ODocument) record, getDatabase());
              }
              break;
          }
        }
        // RESET TRACKING
        if (record instanceof ODocument && ((ODocument) record).isTrackingChanges()) {
          ODocumentInternal.clearTrackData(((ODocument) record));
        }

      } catch (Exception e) {
        if (callHooks) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(ORecordHook.TYPE.CREATE_FAILED, record);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(ORecordHook.TYPE.UPDATE_FAILED, record);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(ORecordHook.TYPE.DELETE_FAILED, record);
              break;
          }
        }

        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + record.getIdentity()), e);
      }
    } finally {
      if (callHooks) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_CREATION, record);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_UPDATE, record);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, record);
            break;
        }
      }
    }
  }

  public void assignClusters() {
    for (ORecordOperation entry : recordOperations.values()) {
      ORecordId rid = (ORecordId) entry.getRID();
      ORecord record = entry.getRecord();
      if (!rid.isPersistent() && !rid.isTemporary()) {
        ORecordId oldRid = rid.copy();
        ORecordInternal.onBeforeIdentityChanged(record);
        database.assignAndCheckCluster(record, null);
        txGeneratedRealRecordIdMap.put(rid.copy(), oldRid);
        ORecordInternal.onAfterIdentityChanged(record);
      }
    }
  }
}
