package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BeginTransaction38Request implements BinaryRequest<BeginTransactionResponse> {

  private long txId;
  private boolean usingLog;
  private boolean hasContent;
  private List<RecordOperationRequest> operations;

  public BeginTransaction38Request(
      DatabaseSessionInternal db, long txId,
      boolean hasContent,
      boolean usingLog,
      Iterable<RecordOperation> operations,
      Map<String, FrontendTransactionIndexChanges> indexChanges) {
    super();
    this.txId = txId;
    this.hasContent = hasContent;
    this.usingLog = usingLog;
    this.operations = new ArrayList<>();

    if (hasContent) {
      for (var txEntry : operations) {
        var request = new RecordOperationRequest();
        request.setType(txEntry.type);
        request.setVersion(txEntry.record.getVersion());
        request.setId(txEntry.record.getIdentity());
        request.setRecordType(RecordInternal.getRecordType(db, txEntry.record));
        switch (txEntry.type) {
          case RecordOperation.CREATED:
            request.setRecord(
                RecordSerializerNetworkV37Client.INSTANCE.toStream(db, txEntry.record));
            request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
            break;
          case RecordOperation.UPDATED:
            if (EntityImpl.RECORD_TYPE == RecordInternal.getRecordType(db, txEntry.record)) {
              request.setRecordType(DocumentSerializerDelta.DELTA_RECORD_TYPE);
              var delta = DocumentSerializerDelta.instance();
              request.setRecord(delta.serializeDelta(db, (EntityImpl) txEntry.record));
            } else {
              request.setRecord(
                  RecordSerializerNetworkV37.INSTANCE.toStream(db, txEntry.record));
            }
            request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
            break;
        }
        this.operations.add(request);
      }
    }
  }

  public BeginTransaction38Request() {
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    // from 3.0 the the serializer is bound to the protocol
    var serializer = RecordSerializerNetworkV37Client.INSTANCE;

    network.writeLong(txId);
    network.writeBoolean(hasContent);
    network.writeBoolean(usingLog);
    if (hasContent) {
      for (var txEntry : operations) {
        writeTransactionEntry(network, txEntry);
      }

      // END OF RECORD ENTRIES
      network.writeByte((byte) 0);
    }
  }

  @Override
  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    txId = channel.readLong();
    hasContent = channel.readBoolean();
    usingLog = channel.readBoolean();
    operations = new ArrayList<>();
    if (hasContent) {
      byte hasEntry;
      do {
        hasEntry = channel.readByte();
        if (hasEntry == 1) {
          var entry = readTransactionEntry(channel);
          operations.add(entry);
        }
      } while (hasEntry == 1);
    }
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_BEGIN;
  }

  @Override
  public BeginTransactionResponse createResponse() {
    return new BeginTransactionResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeBeginTransaction38(this);
  }

  @Override
  public String getDescription() {
    return "Begin Transaction";
  }

  public List<RecordOperationRequest> getOperations() {
    return operations;
  }

  public long getTxId() {
    return txId;
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public boolean isHasContent() {
    return hasContent;
  }

  static RecordOperationRequest readTransactionEntry(ChannelDataInput channel)
      throws IOException {
    var entry = new RecordOperationRequest();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setRecordType(channel.readByte());
    switch (entry.getType()) {
      case RecordOperation.CREATED:
        entry.setRecord(channel.readBytes());
        break;
      case RecordOperation.UPDATED:
        entry.setVersion(channel.readVersion());
        entry.setRecord(channel.readBytes());
        entry.setContentChanged(channel.readBoolean());
        break;
      case RecordOperation.DELETED:
        entry.setVersion(channel.readVersion());
        break;
      default:
        break;
    }
    return entry;
  }

  static void writeTransactionEntry(
      final ChannelDataOutput iNetwork, final RecordOperationRequest txEntry) throws IOException {
    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case RecordOperation.CREATED:
        iNetwork.writeBytes(txEntry.getRecord());
        break;

      case RecordOperation.UPDATED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getRecord());
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case RecordOperation.DELETED:
        iNetwork.writeVersion(txEntry.getVersion());
        break;
    }
  }
}
