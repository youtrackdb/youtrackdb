package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
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
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeByte(recordType);
    channel.writeVersion(version);
    channel.writeBytes(record);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
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
