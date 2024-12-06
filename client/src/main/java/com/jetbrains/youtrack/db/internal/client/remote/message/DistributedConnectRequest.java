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
public class DistributedConnectRequest implements BinaryRequest<DistributedConnectResponse> {

  private int distributedProtocolVersion;
  private String username;
  private String password;

  public DistributedConnectRequest() {
  }

  public DistributedConnectRequest(
      int distributedProtocolVersion, String username, String password) {
    this.distributedProtocolVersion = distributedProtocolVersion;
    this.username = username;
    this.password = password;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeInt(distributedProtocolVersion);
    network.writeString(username);
    network.writeString(password);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    distributedProtocolVersion = channel.readInt();
    username = channel.readString();
    password = channel.readString();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.DISTRIBUTED_CONNECT;
  }

  @Override
  public DistributedConnectResponse createResponse() {
    return new DistributedConnectResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeDistributedConnect(this);
  }

  @Override
  public String getDescription() {
    return "distributed connect";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public int getDistributedProtocolVersion() {
    return distributedProtocolVersion;
  }
}
