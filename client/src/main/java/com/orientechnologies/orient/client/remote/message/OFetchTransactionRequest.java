package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OFetchTransactionRequest implements OBinaryRequest<OFetchTransactionResponse> {

  private int txId;

  public OFetchTransactionRequest() {
  }

  public OFetchTransactionRequest(int txId) {
    this.txId = txId;
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeInt(txId);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    this.txId = channel.readInt();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_FETCH;
  }

  @Override
  public OFetchTransactionResponse createResponse() {
    return new OFetchTransactionResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeFetchTransaction(this);
  }

  @Override
  public String getDescription() {
    return "Fetch Transaction";
  }

  public int getTxId() {
    return txId;
  }
}
