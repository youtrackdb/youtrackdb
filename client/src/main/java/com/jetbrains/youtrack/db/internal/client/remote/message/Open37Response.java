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
public class Open37Response implements BinaryResponse {

  private int sessionId;
  private byte[] sessionToken;

  public Open37Response() {
  }

  public Open37Response(int sessionId, byte[] sessionToken) {
    this.sessionId = sessionId;
    this.sessionToken = sessionToken;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    channel.writeBytes(sessionToken);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getSessionToken() {
    return sessionToken;
  }
}
