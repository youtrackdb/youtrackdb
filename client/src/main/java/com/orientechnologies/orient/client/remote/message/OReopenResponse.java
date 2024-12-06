package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OReopenResponse implements OBinaryResponse {

  private int sessionId;

  public OReopenResponse() {
  }

  public OReopenResponse(int sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
  }

  public int getSessionId() {
    return sessionId;
  }
}
