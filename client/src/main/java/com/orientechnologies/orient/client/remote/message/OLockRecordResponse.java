package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OLockRecordResponse implements OBinaryResponse {

  private byte recordType;
  private int version;
  private byte[] record;

  public OLockRecordResponse() {
  }

  public OLockRecordResponse(byte recordType, int version, byte[] record) {
    this.recordType = recordType;
    this.version = version;
    this.record = record;
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeByte(recordType);
    channel.writeVersion(version);
    channel.writeBytes(record);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    this.recordType = network.readByte();
    this.version = network.readVersion();
    this.record = network.readBytes();
  }

  public byte getRecordType() {
    return recordType;
  }

  public int getVersion() {
    return version;
  }

  public byte[] getRecord() {
    return record;
  }
}
