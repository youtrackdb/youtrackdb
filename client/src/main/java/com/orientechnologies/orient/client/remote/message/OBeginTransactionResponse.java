package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OBeginTransactionResponse implements OBinaryResponse {

  private long txId;
  private Map<YTRID, YTRID> updatedIds;

  public OBeginTransactionResponse(long txId, Map<YTRID, YTRID> updatedIds) {
    this.txId = txId;
    this.updatedIds = updatedIds;
  }

  public OBeginTransactionResponse() {
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);
    channel.writeInt(updatedIds.size());

    for (Map.Entry<YTRID, YTRID> ids : updatedIds.entrySet()) {
      channel.writeRID(ids.getKey());
      channel.writeRID(ids.getValue());
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    txId = network.readLong();
    int size = network.readInt();
    updatedIds = new HashMap<>(size);
    while (size-- > 0) {
      YTRID key = network.readRID();
      YTRID value = network.readRID();
      updatedIds.put(key, value);
    }
  }

  public long getTxId() {
    return txId;
  }

  public Map<YTRID, YTRID> getUpdatedIds() {
    return updatedIds;
  }
}
