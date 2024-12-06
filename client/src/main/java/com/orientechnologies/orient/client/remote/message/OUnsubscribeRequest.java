package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OUnsubscribeRequest implements OBinaryRequest<OUnsubscribeResponse> {

  private byte unsubscribeMessage;
  private OBinaryRequest<? extends OBinaryResponse> unsubscribeRequest;

  public OUnsubscribeRequest(OBinaryRequest<? extends OBinaryResponse> unsubscribeRequest) {
    this.unsubscribeMessage = unsubscribeRequest.getCommand();
    this.unsubscribeRequest = unsubscribeRequest;
  }

  public OUnsubscribeRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeByte(unsubscribeMessage);
    unsubscribeRequest.write(database, network, session);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    unsubscribeMessage = channel.readByte();
    unsubscribeRequest = createBinaryRequest(unsubscribeMessage);
    unsubscribeRequest.read(db, channel, protocolVersion, serializer);
  }

  private OBinaryRequest<? extends OBinaryResponse> createBinaryRequest(byte message) {
    if (message == ChannelBinaryProtocol.UNSUBSCRIBE_PUSH_LIVE_QUERY) {
      return new OUnsubscribeLiveQueryRequest();
    }

    throw new DatabaseException("Unknown message response for code:" + message);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.UNSUBSCRIBE_PUSH;
  }

  @Override
  public OUnsubscribeResponse createResponse() {
    return new OUnsubscribeResponse(unsubscribeRequest.createResponse());
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeUnsubscribe(this);
  }

  @Override
  public String getDescription() {
    return "Unsubscribe from a push request";
  }

  public OBinaryRequest<? extends OBinaryResponse> getUnsubscribeRequest() {
    return unsubscribeRequest;
  }
}
