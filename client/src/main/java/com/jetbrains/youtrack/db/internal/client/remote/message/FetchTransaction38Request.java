package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class FetchTransaction38Request implements BinaryRequest<FetchTransaction38Response> {

  private long txId;

  public FetchTransaction38Request() {
  }

  public FetchTransaction38Request(long txId) {
    this.txId = txId;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeLong(txId);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    this.txId = channel.readLong();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_FETCH;
  }

  @Override
  public FetchTransaction38Response createResponse() {
    return new FetchTransaction38Response();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
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
