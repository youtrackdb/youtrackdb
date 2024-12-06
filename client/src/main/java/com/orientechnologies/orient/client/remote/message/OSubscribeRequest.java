package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OSubscribeRequest implements OBinaryRequest<OSubscribeResponse> {

  private byte pushMessage;
  private OBinaryRequest<? extends OBinaryResponse> pushRequest;

  public OSubscribeRequest() {
  }

  public OSubscribeRequest(OBinaryRequest<? extends OBinaryResponse> request) {
    this.pushMessage = request.getCommand();
    this.pushRequest = request;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeByte(pushMessage);
    pushRequest.write(database, network, session);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    pushMessage = channel.readByte();
    pushRequest = createBinaryRequest(pushMessage);
    pushRequest.read(db, channel, protocolVersion, serializer);
  }

  private OBinaryRequest<? extends OBinaryResponse> createBinaryRequest(byte message) {
    switch (message) {
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_DISTRIB_CONFIG:
        return new OSubscribeDistributedConfigurationRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_LIVE_QUERY:
        return new OSubscribeLiveQueryRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_STORAGE_CONFIG:
        return new OSubscribeStorageConfigurationRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_SCHEMA:
        return new OSubscribeSchemaRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_INDEX_MANAGER:
        return new OSubscribeIndexManagerRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_FUNCTIONS:
        return new OSubscribeFunctionsRequest();
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH_SEQUENCES:
        return new OSubscribeSequencesRequest();
    }

    throw new DatabaseException("Unknown message response for code:" + message);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.SUBSCRIBE_PUSH;
  }

  @Override
  public OSubscribeResponse createResponse() {
    return new OSubscribeResponse(pushRequest.createResponse());
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribe(this);
  }

  public byte getPushMessage() {
    return pushMessage;
  }

  public OBinaryRequest<? extends OBinaryResponse> getPushRequest() {
    return pushRequest;
  }

  @Override
  public String getDescription() {
    return "Subscribe to push message";
  }
}
