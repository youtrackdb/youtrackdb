package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class ORecordExistsRequest implements OBinaryRequest<ORecordExistsResponse> {

  private ORID recordId;

  public ORecordExistsRequest() {
  }

  public ORecordExistsRequest(ORID recordId) {
    this.recordId = recordId;
  }

  @Override
  public void write(ODatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeRID(recordId);
  }

  @Override
  public void read(ODatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    recordId = channel.readRID();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_EXISTS;
  }

  @Override
  public String getDescription() {
    return "Check if record exists in storage";
  }

  public ORID getRecordId() {
    return recordId;
  }

  @Override
  public ORecordExistsResponse createResponse() {
    return new ORecordExistsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeRecordExists(this);
  }
}
