package com.orientechnologies.orient.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OPushStorageConfigurationRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private final OStorageConfigurationPayload payload;

  public OPushStorageConfigurationRequest() {
    payload = new OStorageConfigurationPayload();
  }

  public OPushStorageConfigurationRequest(StorageConfiguration configuration) {
    payload = new OStorageConfigurationPayload(configuration);
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel)
      throws IOException {
    payload.write(channel);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network) throws IOException {
    payload.read(network);
  }

  @Override
  public OBinaryPushResponse execute(DatabaseSessionInternal session,
      ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateStorageConfig(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_STORAGE_CONFIG;
  }

  public OStorageConfigurationPayload getPayload() {
    return payload;
  }
}
