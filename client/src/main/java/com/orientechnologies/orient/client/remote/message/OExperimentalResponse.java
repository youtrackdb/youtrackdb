package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public class OExperimentalResponse implements OBinaryResponse {

  private OBinaryResponse response;

  public OExperimentalResponse() {
  }

  public OExperimentalResponse(OBinaryResponse response) {
    this.response = response;
  }

  @Override
  public void write(ODatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    response.write(session, channel, protocolVersion, serializer);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    response.read(network, session);
  }

  public OBinaryResponse getResponse() {
    return response;
  }
}
