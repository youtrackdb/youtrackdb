package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OFetchTransaction38Request implements OBinaryRequest<OFetchTransaction38Response> {

  private int txId;

  public OFetchTransaction38Request() {
  }

  public OFetchTransaction38Request(int txId) {
    this.txId = txId;
  }

  @Override
  public void write(ODatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeInt(txId);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.txId = channel.readInt();
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

  public int getTxId() {
    return txId;
  }
}
