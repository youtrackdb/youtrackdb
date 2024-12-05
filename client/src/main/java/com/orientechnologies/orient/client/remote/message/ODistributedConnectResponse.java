package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;

/**
 *
 */
public class ODistributedConnectResponse implements OBinaryResponse {

  private int sessionId;
  private byte[] token;
  private int distributedProtocolVersion;

  public ODistributedConnectResponse(int sessionId, byte[] token, int distributedProtocolVersion) {
    this.sessionId = sessionId;
    this.token = token;
    this.distributedProtocolVersion = distributedProtocolVersion;
  }

  public ODistributedConnectResponse() {
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    channel.writeInt(distributedProtocolVersion);
    channel.writeBytes(token);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
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
