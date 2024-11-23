package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
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
public class OSubscribeMetadataRequest implements OBinaryRequest<OSubscribeMetadataResponse> {

  @Override
  public void write(ODatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
  }

  @Override
  public byte getCommand() {
    return 0;
  }

  @Override
  public OSubscribeMetadataResponse createResponse() {
    return new OSubscribeMetadataResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return null;
  }

  @Override
  public String getDescription() {
    return "Subscribe Metadata";
  }
}
