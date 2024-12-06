package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OShutdownRequest implements OBinaryRequest<OBinaryResponse> {

  private String rootUser;
  private String rootPassword;

  public OShutdownRequest(String rootUser, String rootPassword) {
    super();
    this.rootUser = rootUser;
    this.rootPassword = rootPassword;
  }

  public OShutdownRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeString(rootUser);
    network.writeString(rootPassword);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    rootUser = channel.readString();
    rootPassword = channel.readString();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_SHUTDOWN;
  }

  @Override
  public String getDescription() {
    return "Shutdown Server";
  }

  public String getRootPassword() {
    return rootPassword;
  }

  public String getRootUser() {
    return rootUser;
  }

  @Override
  public OBinaryResponse createResponse() {
    return null;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeShutdown(this);
  }
}
