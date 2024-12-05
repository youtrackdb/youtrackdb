package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
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
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeByte(unsubscribeMessage);
    unsubscribeRequest.write(database, network, session);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    unsubscribeMessage = channel.readByte();
    unsubscribeRequest = createBinaryRequest(unsubscribeMessage);
    unsubscribeRequest.read(db, channel, protocolVersion, serializer);
  }

  private OBinaryRequest<? extends OBinaryResponse> createBinaryRequest(byte message) {
    if (message == OChannelBinaryProtocol.UNSUBSCRIBE_PUSH_LIVE_QUERY) {
      return new OUnsubscribeLiveQueryRequest();
    }

    throw new YTDatabaseException("Unknown message response for code:" + message);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.UNSUBSCRIBE_PUSH;
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
