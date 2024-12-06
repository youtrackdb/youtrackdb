package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperation38Response;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializerDelta;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OFetchTransaction38Response implements OBinaryResponse {

  private long txId;
  private List<ORecordOperation38Response> operations;
  private List<IndexChange> indexChanges;

  public OFetchTransaction38Response() {
  }

  public OFetchTransaction38Response(
      DatabaseSessionInternal session, long txId,
      Iterable<RecordOperation> operations,
      Map<String, FrontendTransactionIndexChanges> indexChanges,
      Map<RID, RID> updatedRids,
      DatabaseSessionInternal database) {
    // In some cases the reference are update twice is not yet possible to guess what is the id in
    // the client

    this.txId = txId;
    this.indexChanges = new ArrayList<>();
    List<ORecordOperation38Response> netOperations = new ArrayList<>();
    for (RecordOperation txEntry : operations) {
      ORecordOperation38Response request = new ORecordOperation38Response();
      request.setType(txEntry.type);
      request.setVersion(txEntry.record.getVersion());
      request.setId(txEntry.getRID());
      RID oldID = updatedRids.get(txEntry.getRID());
      request.setOldId(oldID != null ? oldID : txEntry.getRID());
      request.setRecordType(RecordInternal.getRecordType(txEntry.record));
      if (txEntry.type == RecordOperation.UPDATED
          && txEntry.record instanceof EntityImpl doc) {
        var result =
            database.getStorage()
                .readRecord(database, (RecordId) doc.getIdentity(), false, false, null);

        EntityImpl docFromPersistence = new EntityImpl(doc.getIdentity());
        docFromPersistence.fromStream(result.buffer);
        request.setOriginal(
            RecordSerializerNetworkV37Client.INSTANCE.toStream(session, docFromPersistence));
        DocumentSerializerDelta delta = DocumentSerializerDelta.instance();
        request.setRecord(delta.serializeDelta(doc));
      } else {
        request.setRecord(
            RecordSerializerNetworkV37Client.INSTANCE.toStream(session, txEntry.record));
      }
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

    for (ORecordOperation38Response txEntry : operations) {
      writeTransactionEntry(channel, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    channel.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    OMessageHelper.writeTransactionIndexChanges(
        channel, (RecordSerializerNetworkV37) serializer, indexChanges);
  }

  static void writeTransactionEntry(
      final ChannelDataOutput iNetwork,
      final ORecordOperation38Response txEntry,
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
        iNetwork.writeBytes(txEntry.getOriginal());
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
      OStorageRemoteSession session) throws IOException {
    RecordSerializerNetworkV37Client serializer = RecordSerializerNetworkV37Client.INSTANCE;
    txId = network.readLong();
    operations = new ArrayList<>();
    byte hasEntry;
    do {
      hasEntry = network.readByte();
      if (hasEntry == 1) {
        ORecordOperation38Response entry = readTransactionEntry(network, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    // RECEIVE MANUAL INDEX CHANGES
    this.indexChanges = OMessageHelper.readTransactionIndexChanges(db, network, serializer);
  }

  static ORecordOperation38Response readTransactionEntry(
      ChannelDataInput channel, RecordSerializer ser) throws IOException {
    ORecordOperation38Response entry = new ORecordOperation38Response();
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
        entry.setOriginal(channel.readBytes());
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

  public List<ORecordOperation38Response> getOperations() {
    return operations;
  }

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }
}
