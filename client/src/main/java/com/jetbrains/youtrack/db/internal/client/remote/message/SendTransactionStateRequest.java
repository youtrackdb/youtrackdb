package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class SendTransactionStateRequest implements BinaryRequest<SendTransactionStateResponse> {

  private long txId;

  @Nonnull
  private final List<RecordOperationRequest> operations;

  public SendTransactionStateRequest() {
    operations = new ArrayList<>();
  }

  public SendTransactionStateRequest(DatabaseSessionInternal session, long txId,
      Iterable<RecordOperation> operations) {
    this.txId = txId;
    this.operations = new ArrayList<>();

    for (var txEntry : operations) {
      var request = new RecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.record.getIdentity());
      request.setRecordType(RecordInternal.getRecordType(session, txEntry.record));

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
      StorageRemoteSession session) throws IOException {
    network.writeLong(txId);

    for (var txEntry : operations) {
      BeginTransaction38Request.writeTransactionEntry(network, txEntry);
    }

    //flag of end of entries
    network.writeByte((byte) 0);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    txId = channel.readLong();
    operations.clear();

    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        var entry = BeginTransaction38Request.readTransactionEntry(channel);
        operations.add(entry);
      }
    } while (hasEntry == 1);

  }

  @Override
  public SendTransactionStateResponse createResponse() {
    return new SendTransactionStateResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
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
  public List<RecordOperationRequest> getOperations() {
    return operations;
  }
}
