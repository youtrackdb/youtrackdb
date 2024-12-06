package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OBeginTransactionResponse implements OBinaryResponse {

  private long txId;
  private Map<RID, RID> updatedIds;

  public OBeginTransactionResponse(long txId, Map<RID, RID> updatedIds) {
    this.txId = txId;
    this.updatedIds = updatedIds;
  }

  public OBeginTransactionResponse() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);
    channel.writeInt(updatedIds.size());

    for (Map.Entry<RID, RID> ids : updatedIds.entrySet()) {
      channel.writeRID(ids.getKey());
      channel.writeRID(ids.getValue());
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    txId = network.readLong();
    int size = network.readInt();
    updatedIds = new HashMap<>(size);
    while (size-- > 0) {
      RID key = network.readRID();
      RID value = network.readRID();
      updatedIds.put(key, value);
    }
  }

  public long getTxId() {
    return txId;
  }

  public Map<RID, RID> getUpdatedIds() {
    return updatedIds;
  }
}
