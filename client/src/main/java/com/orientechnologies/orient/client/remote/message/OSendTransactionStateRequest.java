package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class OSendTransactionStateRequest implements OBinaryRequest<OSendTransactionStateResponse> {

  private long txId;

  @Nonnull
  private final List<ORecordOperationRequest> operations;

  public OSendTransactionStateRequest() {
    operations = new ArrayList<>();
  }

  public OSendTransactionStateRequest(DatabaseSessionInternal session, long txId,
      Iterable<RecordOperation> operations) {
    this.txId = txId;
    this.operations = new ArrayList<>();

    for (RecordOperation txEntry : operations) {
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.record.getIdentity());
      request.setRecordType(RecordInternal.getRecordType(txEntry.record));

      switch (txEntry.type) {
        case RecordOperation.CREATED:
        case RecordOperation.UPDATED:
          request.setRecord(
              RecordSerializerNetworkV37Client.INSTANCE.toStream(session, txEntry.record));
          request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
          break;
      }

      this.operations.add(request);
    }
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeLong(txId);

    for (ORecordOperationRequest txEntry : operations) {
      OBeginTransaction38Request.writeTransactionEntry(network, txEntry);
    }

    //flag of end of entries
    network.writeByte((byte) 0);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    txId = channel.readLong();
    operations.clear();

    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = OBeginTransaction38Request.readTransactionEntry(channel);
        operations.add(entry);
      }
    } while (hasEntry == 1);

  }

  @Override
  public OSendTransactionStateResponse createResponse() {
    return new OSendTransactionStateResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSendTransactionState(this);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE;
  }

  @Override
  public String getDescription() {
    return "Sync state of transaction between session opened on client and its mirror on server";
  }

  public long getTxId() {
    return txId;
  }

  @Nonnull
  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }
}
