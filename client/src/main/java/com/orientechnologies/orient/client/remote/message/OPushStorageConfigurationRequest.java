package com.orientechnologies.orient.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.jetbrains.youtrack.db.internal.core.config.OStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushStorageConfigurationRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private final OStorageConfigurationPayload payload;

  public OPushStorageConfigurationRequest() {
    payload = new OStorageConfigurationPayload();
  }

  public OPushStorageConfigurationRequest(OStorageConfiguration configuration) {
    payload = new OStorageConfigurationPayload(configuration);
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel)
      throws IOException {
    payload.write(channel);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network) throws IOException {
    payload.read(network);
  }

  @Override
  public OBinaryPushResponse execute(YTDatabaseSessionInternal session,
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
