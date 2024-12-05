package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
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

  public OSendTransactionStateRequest(YTDatabaseSessionInternal session, long txId,
      Iterable<ORecordOperation> operations) {
    this.txId = txId;
    this.operations = new ArrayList<>();

    for (ORecordOperation txEntry : operations) {
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.record.getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.record));

      switch (txEntry.type) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          request.setRecord(
              ORecordSerializerNetworkV37Client.INSTANCE.toStream(session, txEntry.record));
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.record));
          break;
      }

      this.operations.add(request);
    }
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeLong(txId);

    for (ORecordOperationRequest txEntry : operations) {
      OBeginTransaction38Request.writeTransactionEntry(network, txEntry);
    }

    //flag of end of entries
    network.writeByte((byte) 0);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
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
    return OChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE;
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
