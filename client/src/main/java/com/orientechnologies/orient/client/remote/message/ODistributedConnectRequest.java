package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;

/**
 *
 */
public class ODistributedConnectRequest implements OBinaryRequest<ODistributedConnectResponse> {

  private int distributedProtocolVersion;
  private String username;
  private String password;

  public ODistributedConnectRequest() {
  }

  public ODistributedConnectRequest(
      int distributedProtocolVersion, String username, String password) {
    this.distributedProtocolVersion = distributedProtocolVersion;
    this.username = username;
    this.password = password;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
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
  public ODistributedConnectResponse createResponse() {
    return new ODistributedConnectResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
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
