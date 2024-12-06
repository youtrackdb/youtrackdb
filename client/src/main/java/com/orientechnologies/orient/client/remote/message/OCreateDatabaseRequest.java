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

public class OCreateDatabaseRequest implements OBinaryRequest<OCreateDatabaseResponse> {

  private String databaseName;
  private String databaseType;
  private String storageMode;
  private String backupPath;

  public OCreateDatabaseRequest(
      String databaseName, String databaseType, String storageMode, String backupPath) {
    this.databaseName = databaseName;
    this.databaseType = databaseType;
    this.storageMode = storageMode;
    this.backupPath = backupPath;
  }

  public OCreateDatabaseRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeString(databaseName);
    network.writeString(databaseType);
    network.writeString(storageMode);
    network.writeString(backupPath);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    this.databaseName = channel.readString();
    this.databaseType = channel.readString();
    this.storageMode = channel.readString();
    if (protocolVersion > 35) {
      this.backupPath = channel.readString();
    }
  }

  @Override
  public String requiredServerRole() {
    return "database.create";
  }

  @Override
  public boolean requireServerUser() {
    return true;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_DB_CREATE;
  }

  @Override
  public String getDescription() {
    return "Create database";
  }

  public String getBackupPath() {
    return backupPath;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getDatabaseType() {
    return databaseType;
  }

  public String getStorageMode() {
    return storageMode;
  }

  @Override
  public OCreateDatabaseResponse createResponse() {
    return new OCreateDatabaseResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor ex) {
    return ex.executeCreateDatabase(this);
  }
}
