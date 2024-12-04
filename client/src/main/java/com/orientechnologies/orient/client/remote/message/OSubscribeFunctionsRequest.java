package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OSubscribeFunctionsRequest implements OBinaryRequest<OSubscribeFunctionsResponse> {

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.SUBSCRIBE_PUSH_FUNCTIONS;
  }

  @Override
  public OSubscribeFunctionsResponse createResponse() {
    return new OSubscribeFunctionsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribeFunctions(this);
  }

  @Override
  public String getDescription() {
    return "Subscribe Distributed Configuration";
  }
}
