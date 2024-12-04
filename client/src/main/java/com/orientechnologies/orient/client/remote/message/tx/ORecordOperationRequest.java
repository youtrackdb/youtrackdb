package com.orientechnologies.orient.client.remote.message.tx;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ORecordOperationRequest {

  private byte type;
  private byte recordType;
  private YTRID id;
  private YTRID oldId;
  private byte[] record;
  private int version;
  private boolean contentChanged;

  public ORecordOperationRequest() {
  }

  public ORecordOperationRequest(
      byte type,
      byte recordType,
      YTRID id,
      YTRID oldId,
      byte[] record,
      int version,
      boolean contentChanged) {
    this.type = type;
    this.recordType = recordType;
    this.id = id;
    this.oldId = oldId;
    this.record = record;
    this.version = version;
    this.contentChanged = contentChanged;
  }

  public YTRID getId() {
    return id;
  }

  public void setId(YTRID id) {
    this.id = id;
  }

  public YTRID getOldId() {
    return oldId;
  }

  public void setOldId(YTRID oldId) {
    this.oldId = oldId;
  }

  public byte[] getRecord() {
    return record;
  }

  public void setRecord(byte[] record) {
    this.record = record;
  }

  public byte getRecordType() {
    return recordType;
  }

  public void setRecordType(byte recordType) {
    this.recordType = recordType;
  }

  public byte getType() {
    return type;
  }

  public void setType(byte type) {
    this.type = type;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public void setContentChanged(boolean contentChanged) {
    this.contentChanged = contentChanged;
  }

  public boolean isContentChanged() {
    return contentChanged;
  }

  public void deserialize(DataInput input) throws IOException {
    type = input.readByte();
    recordType = input.readByte();
    id = YTRecordId.deserialize(input);
    oldId = YTRecordId.deserialize(input);
    if (type != ORecordOperation.DELETED) {
      int size = input.readInt();
      record = new byte[size];
      input.readFully(record);
    }
    version = input.readInt();
    contentChanged = input.readBoolean();
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeByte(type);
    output.writeByte(recordType);
    YTRecordId.serialize(id, output);
    YTRecordId.serialize(oldId, output);
    if (type != ORecordOperation.DELETED) {
      output.writeInt(record.length);
      output.write(record);
    }
    output.writeInt(version);
    output.writeBoolean(contentChanged);
  }
}
