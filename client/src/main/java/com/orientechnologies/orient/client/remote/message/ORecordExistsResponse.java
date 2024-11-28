package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class ORecordExistsResponse implements OBinaryResponse {

  private boolean recordExists;

  public ORecordExistsResponse() {
  }

  public ORecordExistsResponse(boolean recordExists) {
    this.recordExists = recordExists;
  }

  public boolean isRecordExists() {
    return recordExists;
  }

  @Override
  public void write(ODatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeByte((byte) (recordExists ? 1 : 0));
  }

  @Override
  public void read(ODatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    recordExists = network.readByte() != 0;
  }
}
