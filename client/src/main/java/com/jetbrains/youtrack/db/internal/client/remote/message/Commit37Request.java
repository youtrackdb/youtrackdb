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

/**
 *
 */
public class Commit37Request implements BinaryRequest<Commit37Response> {

  private long txId;
  private boolean hasContent;
  private boolean usingLog;
  private List<RecordOperationRequest> operations;

  public Commit37Request() {
  }

  public Commit37Request(
      DatabaseSessionInternal db, long txId,
      boolean hasContent,
      boolean usingLong,
      Iterable<RecordOperation> operations) {
    this.txId = txId;
    this.hasContent = hasContent;
    this.usingLog = usingLong;
    if (hasContent) {
      List<RecordOperationRequest> netOperations = new ArrayList<>();
      for (var txEntry : operations) {
        var request = new RecordOperationRequest();
        request.setType(txEntry.type);
        request.setVersion(txEntry.record.getVersion());
        request.setId(txEntry.record.getIdentity());
        request.setRecordType(RecordInternal.getRecordType(db, txEntry.record));
        switch (txEntry.type) {
          case RecordOperation.CREATED:
          case RecordOperation.UPDATED:
            request.setRecord(
                RecordSerializerNetworkV37Client.INSTANCE.toStream(db, txEntry.record));
            request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
            break;
        }
        netOperations.add(request);
      }
      this.operations = netOperations;
    }
  }

  @Override
  public void write(DatabaseSessionInternal db, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    // from 3.0 the the serializer is bound to the protocol
    var serializer = RecordSerializerNetworkV37Client.INSTANCE;
    network.writeLong(txId);
    network.writeBoolean(hasContent);
    network.writeBoolean(usingLog);
    if (hasContent) {
      for (var txEntry : operations) {
        network.writeByte((byte) 1);
        MessageHelper.writeTransactionEntry(network, txEntry, serializer);
      }

      // END OF RECORD ENTRIES
      network.writeByte((byte) 0);
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    txId = channel.readLong();
    hasContent = channel.readBoolean();
    usingLog = channel.readBoolean();
    if (hasContent) {
      operations = new ArrayList<>();
      byte hasEntry;
      do {
        hasEntry = channel.readByte();
        if (hasEntry == 1) {
          var entry = MessageHelper.readTransactionEntry(channel, serializer);
          operations.add(entry);
        }
      } while (hasEntry == 1);
    }
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }

  @Override
  public Commit37Response createResponse() {
    return new Commit37Response();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCommit37(this);
  }

  @Override
  public String getDescription() {
    return "Commit";
  }

  public long getTxId() {
    return txId;
  }

  public List<RecordOperationRequest> getOperations() {
    return operations;
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public boolean isHasContent() {
    return hasContent;
  }
}
