package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BeginTransactionResponse implements BinaryResponse {

  private long txId;
  private Map<RecordId, RecordId> updatedIds;

  public BeginTransactionResponse(long txId, Map<RecordId, RecordId> updatedIds) {
    this.txId = txId;
    this.updatedIds = updatedIds;
  }

  public BeginTransactionResponse() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);
    channel.writeInt(updatedIds.size());

    for (var ids : updatedIds.entrySet()) {
      channel.writeRID(ids.getKey());
      channel.writeRID(ids.getValue());
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    txId = network.readLong();
    var size = network.readInt();
    updatedIds = new HashMap<>(size);
    while (size-- > 0) {
      var key = network.readRID();
      var value = network.readRID();
      updatedIds.put(key, value);
    }
  }

  public long getTxId() {
    return txId;
  }

  public Map<RecordId, RecordId> getUpdatedIds() {
    return updatedIds;
  }
}
