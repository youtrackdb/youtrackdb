package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class FetchTransactionResponse implements BinaryResponse {

  private long txId;
  private List<RecordOperationRequest> operations;

  public FetchTransactionResponse() {
  }

  public FetchTransactionResponse(
      DatabaseSessionInternal db, long txId,
      Iterable<RecordOperation> operations,
      Map<RecordId, RecordId> updatedRids) {
    // In some cases the reference are update twice is not yet possible to guess what is the id in
    // the client
    var reversed =
        updatedRids.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    this.txId = txId;
    List<RecordOperationRequest> netOperations = new ArrayList<>();
    for (var txEntry : operations) {
      var request = new RecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.getRecordId());
      var oldID = reversed.get(txEntry.getRecordId());
      request.setOldId(oldID != null ? oldID : txEntry.getRecordId());
      request.setRecordType(RecordInternal.getRecordType(db, txEntry.record));
      request.setRecord(
          RecordSerializerNetworkV37Client.INSTANCE.toStream(db, txEntry.record));
      request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
      netOperations.add(request);
    }
    this.operations = netOperations;
  }

  @Override
  public void write(DatabaseSessionInternal db, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);

    for (var txEntry : operations) {
      writeTransactionEntry(channel, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    channel.writeByte((byte) 0);
  }

  static void writeTransactionEntry(
      final ChannelDataOutput iNetwork,
      final RecordOperationRequest txEntry,
      RecordSerializer serializer)
      throws IOException {
    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeRID(txEntry.getOldId());
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
        iNetwork.writeBytes(txEntry.getRecord());
        break;
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    var serializer = RecordSerializerNetworkV37Client.INSTANCE;
    txId = network.readLong();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = network.readByte();
      if (hasEntry == 1) {
        var entry = readTransactionEntry(network, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);
  }

  static RecordOperationRequest readTransactionEntry(
      ChannelDataInput channel, RecordSerializer ser) throws IOException {
    var entry = new RecordOperationRequest();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setOldId(channel.readRID());
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
        entry.setRecord(channel.readBytes());
        break;
      default:
        break;
    }
    return entry;
  }

  public long getTxId() {
    return txId;
  }

  public List<RecordOperationRequest> getOperations() {
    return operations;
  }
}
