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

public class ReleaseDatabaseRequest implements BinaryRequest<ReleaseDatabaseResponse> {

  private String name;
  private String storageType;

  public ReleaseDatabaseRequest(String name, String storageType) {
    this.name = name;
    this.storageType = storageType;
  }

  public ReleaseDatabaseRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeString(name);
    network.writeString(storageType);
  }

  @Override
  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    name = channel.readString();
    storageType = channel.readString();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_DB_RELEASE;
  }

  @Override
  public String requiredServerRole() {
    return "database.release";
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public String getDescription() {
    return "Release Database";
  }

  public String getName() {
    return name;
  }

  public String getStorageType() {
    return storageType;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public ReleaseDatabaseResponse createResponse() {
    return new ReleaseDatabaseResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeReleaseDatabase(this);
  }
}
