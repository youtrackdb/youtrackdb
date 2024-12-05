package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OFetchTransaction38Request implements OBinaryRequest<OFetchTransaction38Response> {

  private long txId;

  public OFetchTransaction38Request() {
  }

  public OFetchTransaction38Request(long txId) {
    this.txId = txId;
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeLong(txId);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    this.txId = channel.readLong();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_FETCH;
  }

  @Override
  public OFetchTransaction38Response createResponse() {
    return new OFetchTransaction38Response();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeFetchTransaction38(this);
  }

  @Override
  public String getDescription() {
    return "Fetch Transaction";
  }

  public long getTxId() {
    return txId;
  }
}
