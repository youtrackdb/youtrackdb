package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class LockRecordResponse implements BinaryResponse {

  private byte recordType;
  private int version;
  private byte[] record;

  public LockRecordResponse() {
  }

  public LockRecordResponse(byte recordType, int version, byte[] record) {
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
      StorageRemoteSession session) throws IOException {
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
