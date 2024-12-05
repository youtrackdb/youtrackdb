package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OCommit37Request implements OBinaryRequest<OCommit37Response> {

  private long txId;
  private boolean hasContent;
  private boolean usingLog;
  private List<ORecordOperationRequest> operations;
  private List<IndexChange> indexChanges;

  public OCommit37Request() {
  }

  public OCommit37Request(
      YTDatabaseSessionInternal session, long txId,
      boolean hasContent,
      boolean usingLong,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> indexChanges) {
    this.txId = txId;
    this.hasContent = hasContent;
    this.usingLog = usingLong;
    if (hasContent) {
      this.indexChanges = new ArrayList<>();
      List<ORecordOperationRequest> netOperations = new ArrayList<>();
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
        netOperations.add(request);
      }
      this.operations = netOperations;

      for (Map.Entry<String, OTransactionIndexChanges> change : indexChanges.entrySet()) {
        this.indexChanges.add(new IndexChange(change.getKey(), change.getValue()));
      }
    }
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    // from 3.0 the the serializer is bound to the protocol
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    network.writeLong(txId);
    network.writeBoolean(hasContent);
    network.writeBoolean(usingLog);
    if (hasContent) {
      for (ORecordOperationRequest txEntry : operations) {
        network.writeByte((byte) 1);
        OMessageHelper.writeTransactionEntry(network, txEntry, serializer);
      }

      // END OF RECORD ENTRIES
      network.writeByte((byte) 0);

      // SEND MANUAL INDEX CHANGES
      OMessageHelper.writeTransactionIndexChanges(network, serializer, indexChanges);
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
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
          ORecordOperationRequest entry = OMessageHelper.readTransactionEntry(channel, serializer);
          operations.add(entry);
        }
      } while (hasEntry == 1);

      // RECEIVE MANUAL INDEX CHANGES
      this.indexChanges =
          OMessageHelper.readTransactionIndexChanges(db,
              channel, (ORecordSerializerNetworkV37) serializer);
    }
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }

  @Override
  public OCommit37Response createResponse() {
    return new OCommit37Response();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCommit37(this);
  }

  @Override
  public String getDescription() {
    return "Commit";
  }

  public long getTxId() {
    return txId;
  }

  public List<IndexChange> getIndexChanges() {
    return indexChanges;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public boolean isHasContent() {
    return hasContent;
  }
}
