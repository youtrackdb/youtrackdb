package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.IndexChange;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
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
  private List<IndexChange> indexChanges;

  public FetchTransactionResponse() {
  }

  public FetchTransactionResponse(
      DatabaseSessionInternal session, long txId,
      Iterable<RecordOperation> operations,
      Map<String, FrontendTransactionIndexChanges> indexChanges,
      Map<RID, RID> updatedRids) {
    // In some cases the reference are update twice is not yet possible to guess what is the id in
    // the client
    Map<RID, RID> reversed =
        updatedRids.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    this.txId = txId;
    this.indexChanges = new ArrayList<>();
    List<RecordOperationRequest> netOperations = new ArrayList<>();
    for (RecordOperation txEntry : operations) {
      RecordOperationRequest request = new RecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.getRID());
      RID oldID = reversed.get(txEntry.getRID());
      request.setOldId(oldID != null ? oldID : txEntry.getRID());
      request.setRecordType(RecordInternal.getRecordType(txEntry.record));
      request.setRecord(
          RecordSerializerNetworkV37Client.INSTANCE.toStream(session, txEntry.record));
      request.setContentChanged(RecordInternal.isContentChanged(txEntry.record));
      netOperations.add(request);
    }
    this.operations = netOperations;

    for (Map.Entry<String, FrontendTransactionIndexChanges> change : indexChanges.entrySet()) {
      this.indexChanges.add(new IndexChange(change.getKey(), change.getValue()));
    }
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);

    for (RecordOperationRequest txEntry : operations) {
      writeTransactionEntry(channel, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    channel.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    MessageHelper.writeTransactionIndexChanges(
        channel, (RecordSerializerNetworkV37) serializer, indexChanges);
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
    RecordSerializerNetworkV37Client serializer = RecordSerializerNetworkV37Client.INSTANCE;
    txId = network.readLong();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = network.readByte();
      if (hasEntry == 1) {
        RecordOperationRequest entry = readTransactionEntry(network, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    // RECEIVE MANUAL INDEX CHANGES
    this.indexChanges = MessageHelper.readTransactionIndexChanges(db, network, serializer);
  }

  static RecordOperationRequest readTransactionEntry(
      ChannelDataInput channel, RecordSerializer ser) throws IOException {
    RecordOperationRequest entry = new RecordOperationRequest();
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

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }
}
