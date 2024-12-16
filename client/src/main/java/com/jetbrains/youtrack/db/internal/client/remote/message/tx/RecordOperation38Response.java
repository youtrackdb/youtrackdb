package com.jetbrains.youtrack.db.internal.client.remote.message.tx;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;

public class RecordOperation38Response {

  private byte type;
  private byte recordType;
  private RecordId id;
  private RecordId oldId;
  private byte[] record;
  private byte[] original;
  private int version;
  private boolean contentChanged;

  public RecordOperation38Response() {
  }

  public RecordOperation38Response(
      byte type,
      byte recordType,
      RecordId id,
      RecordId oldId,
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

  public RecordId getId() {
    return id;
  }

  public void setId(RecordId id) {
    this.id = id;
  }

  public RecordId getOldId() {
    return oldId;
  }

  public void setOldId(RecordId oldId) {
    this.oldId = oldId;
  }

  public byte[] getRecord() {
    return record;
  }

  public void setRecord(byte[] record) {
    this.record = record;
  }

  public byte[] getOriginal() {
    return original;
  }

  public void setOriginal(byte[] original) {
    this.original = original;
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
}
