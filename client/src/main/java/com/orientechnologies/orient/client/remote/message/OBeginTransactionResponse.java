package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OBeginTransactionResponse implements OBinaryResponse {

  private long txId;
  private Map<ORID, ORID> updatedIds;

  public OBeginTransactionResponse(long txId, Map<ORID, ORID> updatedIds) {
    this.txId = txId;
    this.updatedIds = updatedIds;
  }

  public OBeginTransactionResponse() {
  }

  @Override
  public void write(ODatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeLong(txId);
    channel.writeInt(updatedIds.size());

    for (Map.Entry<ORID, ORID> ids : updatedIds.entrySet()) {
      channel.writeRID(ids.getKey());
      channel.writeRID(ids.getValue());
    }
  }

  @Override
  public void read(ODatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    txId = network.readLong();
    int size = network.readInt();
    updatedIds = new HashMap<>(size);
    while (size-- > 0) {
      ORID key = network.readRID();
      ORID value = network.readRID();
      updatedIds.put(key, value);
    }
  }

  public long getTxId() {
    return txId;
  }

  public Map<ORID, ORID> getUpdatedIds() {
    return updatedIds;
  }
}
