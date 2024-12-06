package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OExistsDatabaseResponse implements OBinaryResponse {

  private boolean exists;

  public OExistsDatabaseResponse() {
  }

  public OExistsDatabaseResponse(boolean exists) {
    this.exists = exists;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeBoolean(exists);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    exists = network.readBoolean();
  }

  public boolean isExists() {
    return exists;
  }
}
