package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class ORollbackTransactionRequest implements OBinaryRequest<ORollbackTransactionResponse> {

  private long txId;

  public ORollbackTransactionRequest() {
  }

  public ORollbackTransactionRequest(long id) {
    this.txId = id;
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
    txId = channel.readLong();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_ROLLBACK;
  }

  @Override
  public ORollbackTransactionResponse createResponse() {
    return new ORollbackTransactionResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeRollback(this);
  }

  @Override
  public String getDescription() {
    return "Transaction Rollback";
  }
}
