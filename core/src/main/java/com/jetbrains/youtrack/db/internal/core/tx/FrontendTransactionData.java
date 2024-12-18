package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkDistributed;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FrontendTransactionData {

  private final FrontendTransactionId transactionId;
  private final List<FrontendTransactionDataChange> changes = new ArrayList<>();

  public FrontendTransactionData(FrontendTransactionId transactionId) {
    this.transactionId = transactionId;
  }

  public static FrontendTransactionData read(DataInput dataInput) throws IOException {
    FrontendTransactionId transactionId = FrontendTransactionId.read(dataInput);
    int entries = dataInput.readInt();
    FrontendTransactionData data = new FrontendTransactionData(transactionId);
    while (entries-- > 0) {
      data.changes.add(FrontendTransactionDataChange.deserialize(dataInput));
    }
    return data;
  }

  public void addRecord(byte[] record) {
    try {
      changes.add(
          FrontendTransactionDataChange.deserialize(
              new DataInputStream(new ByteArrayInputStream(record))));
    } catch (IOException e) {
      throw BaseException.wrapException(
          new DatabaseException("error reading transaction data change record"), e);
    }
  }

  public void addChange(FrontendTransactionDataChange change) {
    this.changes.add(change);
  }

  public FrontendTransactionId getTransactionId() {
    return transactionId;
  }

  public List<FrontendTransactionDataChange> getChanges() {
    return changes;
  }

  public void write(DataOutput output) throws IOException {
    transactionId.write(output);
    output.writeInt(changes.size());
    for (FrontendTransactionDataChange change : changes) {
      change.serialize(output);
    }
  }

  public void fill(TransactionInternal transaction, DatabaseSessionInternal database) {
    transaction.fill(
        changes.stream()
            .map(
                (x) -> {
                  RecordOperation operation = new RecordOperation(null, x.getType());
                  // TODO: Handle dirty no changed
                  RecordAbstract record = null;
                  switch (x.getType()) {
                    case RecordOperation.CREATED: {
                      record =
                          RecordSerializerNetworkDistributed.INSTANCE.fromStream(database,
                              x.getRecord().get(), null);
                      RecordInternal.setRecordSerializer(record, database.getSerializer());
                      break;
                    }
                    case RecordOperation.UPDATED: {
                      if (x.getRecordType() == EntityImpl.RECORD_TYPE) {
                        try {
                          record = database.load(x.getId());
                        } catch (RecordNotFoundException rnf) {
                          record = new EntityImpl(database);
                        }

                        ((EntityImpl) record).deserializeFields();
                        EntityInternalUtils.clearTransactionTrackData((EntityImpl) record);
                        DocumentSerializerDelta.instance()
                            .deserializeDelta(database, x.getRecord().get(), (EntityImpl) record);
                        /// Got record with empty deltas, at this level we mark the record dirty
                        // anyway.
                        record.setDirty();
                      } else {
                        record =
                            RecordSerializerNetworkDistributed.INSTANCE.fromStream(database,
                                x.getRecord().get(), null);
                        RecordInternal.setRecordSerializer(record, database.getSerializer());
                      }
                      break;
                    }
                    case RecordOperation.DELETED: {
                      try {
                        record = database.load(x.getId());
                      } catch (RecordNotFoundException rnf) {
                        record =
                            YouTrackDBEnginesManager.instance()
                                .getRecordFactoryManager()
                                .newInstance(x.getRecordType(), x.getId(), database);
                      }
                      break;
                    }
                  }
                  RecordInternal.setIdentity(record, (RecordId) x.getId());
                  RecordInternal.setVersion(record, x.getVersion());
                  operation.record = record;

                  return operation;
                })
            .iterator());
  }

  @Override
  public String toString() {
    return "id:" + transactionId + " nchanges:" + changes.size() + "]";
  }
}
