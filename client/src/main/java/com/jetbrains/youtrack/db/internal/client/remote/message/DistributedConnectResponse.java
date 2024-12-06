package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class DistributedConnectResponse implements BinaryResponse {

  private int sessionId;
  private byte[] token;
  private int distributedProtocolVersion;

  public DistributedConnectResponse(int sessionId, byte[] token, int distributedProtocolVersion) {
    this.sessionId = sessionId;
    this.token = token;
    this.distributedProtocolVersion = distributedProtocolVersion;
  }

  public DistributedConnectResponse() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    channel.writeInt(distributedProtocolVersion);
    channel.writeBytes(token);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    this.sessionId = network.readInt();
    distributedProtocolVersion = network.readInt();
    token = network.readBytes();
  }

  public int getDistributedProtocolVersion() {
    return distributedProtocolVersion;
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getToken() {
    return token;
  }
}
