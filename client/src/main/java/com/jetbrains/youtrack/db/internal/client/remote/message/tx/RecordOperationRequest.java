package com.jetbrains.youtrack.db.internal.client.remote.message.tx;

import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordOperationRequest {

  private byte type;
  private byte recordType;
  private RID id;
  private RID oldId;
  private byte[] record;
  private int version;
  private boolean contentChanged;

  public RecordOperationRequest() {
  }

  public RecordOperationRequest(
      byte type,
      byte recordType,
      RID id,
      RID oldId,
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

  public RID getId() {
    return id;
  }

  public void setId(RID id) {
    this.id = id;
  }

  public RID getOldId() {
    return oldId;
  }

  public void setOldId(RID oldId) {
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
    id = RecordId.deserialize(input);
    oldId = RecordId.deserialize(input);
    if (type != RecordOperation.DELETED) {
      var size = input.readInt();
      record = new byte[size];
      input.readFully(record);
    }
    version = input.readInt();
    contentChanged = input.readBoolean();
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeByte(type);
    output.writeByte(recordType);
    RecordId.serialize(id, output);
    RecordId.serialize(oldId, output);
    if (type != RecordOperation.DELETED) {
      output.writeInt(record.length);
      output.write(record);
    }
    output.writeInt(version);
    output.writeBoolean(contentChanged);
  }
}
