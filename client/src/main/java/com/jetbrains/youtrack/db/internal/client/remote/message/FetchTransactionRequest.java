package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class FetchTransactionRequest implements BinaryRequest<FetchTransactionResponse> {

  private int txId;

  public FetchTransactionRequest() {
  }

  public FetchTransactionRequest(int txId) {
    this.txId = txId;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeInt(txId);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    this.txId = channel.readInt();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_FETCH;
  }

  @Override
  public FetchTransactionResponse createResponse() {
    return new FetchTransactionResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
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
