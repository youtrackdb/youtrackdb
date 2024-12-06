package com.orientechnologies.orient.client.remote.message.tx;

import com.jetbrains.youtrack.db.internal.core.id.RID;

public class ORecordOperation38Response {

  private byte type;
  private byte recordType;
  private RID id;
  private RID oldId;
  private byte[] record;
  private byte[] original;
  private int version;
  private boolean contentChanged;

  public ORecordOperation38Response() {
  }

  public ORecordOperation38Response(
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
